import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { eq } from "drizzle-orm";
import type { Pool } from "mysql2/promise";
import sharp from "sharp";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from "vitest";
import type { MultimodalAnalyzer } from "@/server/model/analyzer";
import type {
  ObjectByteRange,
  ObjectStorage,
  StoreFileInput,
} from "@/server/storage/object-storage";

const execFileAsync = promisify(execFile);
vi.unmock("@/server/observability/store");
const semanticSearchEnabledMock = vi.hoisted(() => vi.fn(() => false));
const searchAnalysisMock = vi.hoisted(() => vi.fn(async (): Promise<Map<string, number>> => new Map()));

vi.mock("@/server/search/chroma", () => ({
  semanticSearchEnabled: semanticSearchEnabledMock,
  indexAnalysis: vi.fn(async () => undefined),
  searchAnalysis: searchAnalysisMock,
  deleteAnalysis: vi.fn(async () => undefined),
}));

try {
  process.loadEnvFile?.(".env");
} catch (error) {
  const code = (error as NodeJS.ErrnoException).code;
  if (code !== "ENOENT") throw error;
}

const testDatabaseUrl = process.env.TEST_DATABASE_URL;
const mysqlPipeline = testDatabaseUrl ? describe : describe.skip;
const applicationTables = [
  "analysis_results",
  "asset_tag_rejections",
  "asset_tags",
  "assets",
  "callback_deliveries",
  "call_logs",
  "call_spans",
  "idempotency_requests",
  "jobs",
  "media_objects",
  "outbox_events",
  "search_index_state",
  "tags",
  "task_item_segments",
  "task_items",
  "task_logs",
  "tasks",
  "video_sources",
] as const;

type DatabaseModule = typeof import("@/server/db");
type Repository = typeof import("@/server/repositories/assets");
type Schema = typeof import("@/server/db/schema");
type Processing = typeof import("@/server/services/processing");
type ApiService = typeof import("@/server/api/v1/default-service");
type SceneClient = import("@/server/scene/client").SceneDetectClient;

function assertDedicatedTestDatabase(url: string) {
  const name = decodeURIComponent(new URL(url).pathname.replace(/^\//, ""));
  if (!name.endsWith("_test")) {
    throw new Error("TEST_DATABASE_URL 必须指向以 _test 结尾的专用测试库。");
  }
}

async function truncateApplicationTables(pool: Pool) {
  const connection = await pool.getConnection();
  try {
    await connection.query("SET FOREIGN_KEY_CHECKS = 0");
    for (const table of applicationTables) {
      await connection.query(`TRUNCATE TABLE \`${table}\``);
    }
  } finally {
    await connection.query("SET FOREIGN_KEY_CHECKS = 1");
    connection.release();
  }
}

function body(bytes: Uint8Array) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

/** CI 中的 ZOS 替身保存真实媒体字节，并提供与生产适配器相同的下载语义。 */
class MemoryObjectStorage implements ObjectStorage {
  readonly objects = new Map<string, { bytes: Buffer; contentType: string }>();

  async storeFile(input: StoreFileInput) {
    const bytes = await fs.readFile(input.filePath);
    this.objects.set(input.key, { bytes, contentType: input.contentType });
    return { key: input.key, sizeBytes: bytes.byteLength, etag: "test-etag" };
  }

  async headObject(key: string) {
    const object = this.required(key);
    return {
      key,
      sizeBytes: object.bytes.byteLength,
      contentType: object.contentType,
      etag: "test-etag",
    };
  }

  async getObject(key: string, range?: ObjectByteRange) {
    const object = this.required(key);
    const start = range?.start ?? 0;
    const end = Math.min(
      range?.end ?? object.bytes.byteLength - 1,
      object.bytes.byteLength - 1,
    );
    const bytes = object.bytes.subarray(start, end + 1);
    return {
      ...(await this.headObject(key)),
      body: body(bytes),
      contentLength: bytes.byteLength,
      ...(range
        ? { contentRange: `bytes ${start}-${end}/${object.bytes.byteLength}` }
        : {}),
    };
  }

  async downloadToFile(key: string, destinationPath: string) {
    const object = this.required(key);
    await fs.mkdir(path.dirname(destinationPath), { recursive: true });
    await fs.writeFile(destinationPath, object.bytes);
    return this.headObject(key);
  }

  async deleteObject(key: string) {
    this.objects.delete(key);
  }

  private required(key: string) {
    const object = this.objects.get(key);
    if (!object) throw new Error(`object not found: ${key}`);
    return object;
  }
}

async function createVideo(filePath: string, color: string, duration = 0.4) {
  await execFileAsync("ffmpeg", [
    "-v",
    "error",
    "-f",
    "lavfi",
    "-i",
    `color=c=${color}:s=64x64:d=${duration}`,
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    "-movflags",
    "+faststart",
    "-y",
    filePath,
  ]);
}

function splitManifest(
  taskId: string,
  segments: Array<{ bytes: Buffer; duration: number }>,
) {
  let cursor = 0;
  return {
    taskId,
    originalFilename: "parent.mp4",
    durationSeconds: segments.reduce((sum, segment) => sum + segment.duration, 0),
    sceneCount: segments.length,
    segments: segments.map((segment, offset) => {
      const startSeconds = cursor;
      cursor += segment.duration;
      return {
        index: offset + 1,
        startSeconds,
        endSeconds: cursor,
        durationSeconds: segment.duration,
        startFrame: offset * 12,
        endFrame: (offset + 1) * 12,
        sizeBytes: segment.bytes.byteLength,
        filename: `segment-${String(offset + 1).padStart(3, "0")}.mp4`,
        downloadUrl: `/api/v1/videos/split/${taskId}/segments/${offset + 1}`,
      };
    }),
  };
}

function fakeSceneClient(
  manifest: ReturnType<typeof splitManifest>,
  segmentBytes: Buffer[],
) {
  const request = vi.fn<typeof fetch>(async (input, init) => {
    const url = new URL(String(input));
    if (url.pathname === "/api/v1/videos/split" && init?.method === "POST") {
      return Response.json(manifest);
    }
    const segment = url.pathname.match(/\/segments\/(\d+)$/);
    if (segment) {
      const bytes = segmentBytes[Number(segment[1]) - 1]!;
      return new Response(bytes.buffer.slice(
        bytes.byteOffset,
        bytes.byteOffset + bytes.byteLength,
      ) as ArrayBuffer);
    }
    if (
      url.pathname === `/api/v1/videos/split/${manifest.taskId}` &&
      init?.method === "DELETE"
    ) {
      return new Response(null, { status: 204 });
    }
    return new Response(null, { status: 404 });
  });
  return { request };
}

const analyzer: MultimodalAnalyzer = {
  async analyze(input) {
    return input.mediaType === "image"
      ? {
          result: {
            kind: "image" as const,
            description: "测试图片分析完成",
            tags: {
              scene: ["测试场景"],
              object: ["图片"],
              person: [],
              style: ["简洁"],
              color_composition: ["蓝色"],
            },
            ocr: { text: null, unavailableReason: "无文字" },
          },
          model: { protocol: "openai_chat_completions" as const, name: "fake-vlm" },
        }
      : {
          result: {
            kind: "video" as const,
            description: `切片 ${input.assetId} 关键帧分析完成`,
            topics: ["测试视频"],
            tags: { scene: ["彩色画面"], person: [], form: ["短视频"] },
            visualSegments: [{ startSeconds: 0, endSeconds: 0.4, summary: "彩色画面" }],
            keyMoments: [{ seconds: 0, summary: "首帧" }],
            timeline: [{ startSeconds: 0, endSeconds: 0.4, summary: "彩色画面" }],
          },
          model: { protocol: "openai_chat_completions" as const, name: "fake-vlm" },
        };
  },
};

mysqlPipeline("API v1 完整媒体管线", () => {
  let temporaryRoot: string;
  let database: DatabaseModule;
  let repository: Repository;
  let schema: Schema;
  let processing: Processing;
  let api: ApiService;
  let storage: MemoryObjectStorage;

  beforeAll(async () => {
    if (!testDatabaseUrl) return;
    assertDedicatedTestDatabase(testDatabaseUrl);
    temporaryRoot = await fs.mkdtemp(path.join(os.tmpdir(), "assets-pipeline-"));
    process.env.DATABASE_URL = testDatabaseUrl;
    process.env.MEDIA_ROOT = path.join(temporaryRoot, "media");
    process.env.SCENE_DETECT_WORKSPACE_ROOT = path.join(temporaryRoot, "scenes");
    process.env.SCENE_DETECT_ENABLED = "true";
    process.env.SCENE_SEGMENT_MAX_BYTES = String(10 * 1024 * 1024);
    process.env.EMBEDDING_BASE_URL = "";
    process.env.EMBEDDING_MODEL = "";
    process.env.ZOS_BUCKET = "test-bucket";

    const { initializeDatabase } = await import("@/server/db/migrations");
    const connection = await initializeDatabase({
      url: testDatabaseUrl,
      sslCaPath: process.env.DATABASE_SSL_CA_PATH || undefined,
      poolSize: 4,
    });
    // 从此处开始的源码导入都共用同一个测试库单例。
    database = await import("@/server/db");
    repository = await import("@/server/repositories/assets");
    schema = await import("@/server/db/schema");
    processing = await import("@/server/services/processing");
    api = await import("@/server/api/v1/default-service");
    await connection.pool.end();
  }, 30_000);

  beforeEach(async () => {
    semanticSearchEnabledMock.mockReturnValue(false);
    searchAnalysisMock.mockResolvedValue(new Map());
    storage = new MemoryObjectStorage();
    await truncateApplicationTables(database.pool);
    await fs.rm(process.env.MEDIA_ROOT!, { recursive: true, force: true });
    await fs.rm(process.env.SCENE_DETECT_WORKSPACE_ROOT!, {
      recursive: true,
      force: true,
    });
    vi.clearAllMocks();
  });

  afterAll(async () => {
    if (database?.pool) await database.pool.end();
    if (temporaryRoot) await fs.rm(temporaryRoot, { recursive: true, force: true });
  });

  test("匹配任务完成后可查询入参出参和增量日志，任务删除级联清理日志", async () => {
    const { createMatchTask, processMatchJob } = await import("@/server/services/match");
    const { getMonitorDetail, getTaskLogs, listMonitorTasks } = await import("@/server/services/task-monitor");
    const { matchRequestSchema, monitorQuerySchema, taskLogQuerySchema } = await import("@/shared/monitor-contracts");
    semanticSearchEnabledMock.mockReturnValue(true);
    const created = await createMatchTask(matchRequestSchema.parse({ query: "海边汽车", filter: { user_scope: { mode: "user", user_id: "match-user" } } }));
    expect(created).toMatchObject({ task_type: "match", status: "queued", phase: "matching" });
    const claimed = await repository.claimNextJob();
    expect(claimed?.type).toBe("match");
    await processMatchJob(claimed!);
    const detail = await getMonitorDetail(created.task_id);
    expect(detail.input).toMatchObject({ query: "海边汽车" });
    expect(detail.output).toMatchObject({ status: "done", result: { items: [], matched_count: 0 } });
    expect(detail.jobs[0]).toMatchObject({ status: "done", attempt: 1 });
    const page = await listMonitorTasks(monitorQuerySchema.parse({ type: "match", status: "done", user_id: "match-user" }));
    expect(page).toMatchObject({ total: 1, counts: { done: 1, failed: 0 } });
    const first = await getTaskLogs(created.task_id, taskLogQuerySchema.parse({ limit: 1 }));
    expect(first.has_more).toBe(true);
    const rest = await getTaskLogs(created.task_id, taskLogQuerySchema.parse({ after: first.next_after }));
    expect(rest.items.every((log) => log.id > first.next_after)).toBe(true);
    expect(rest.items.at(-1)?.message).toContain("匹配完成");
    await database.db.delete(schema.tasks).where(eq(schema.tasks.id, created.task_id));
    expect(await database.db.select().from(schema.taskLogs)).toHaveLength(0);
    await expect(getMonitorDetail(created.task_id)).rejects.toMatchObject({ status: 404 });
  });

  test("匹配依赖失败持久化错误，旧租约不得覆盖恢复后的任务", async () => {
    const { createMatchTask, processMatchJob } = await import("@/server/services/match");
    const { getMonitorDetail, getTaskLogs } = await import("@/server/services/task-monitor");
    const { matchRequestSchema, taskLogQuerySchema } = await import("@/shared/monitor-contracts");
    const created = await createMatchTask(matchRequestSchema.parse({ query: "海边" }));
    const oldJob = await repository.claimNextJob();
    await database.db.update(schema.jobs).set({ claimedAt: new Date(0) }).where(eq(schema.jobs.id, oldJob!.id));
    await repository.recoverStaleJobs();
    const newJob = await repository.claimNextJob();
    expect(newJob?.attempt).toBe(2);
    await processMatchJob(oldJob!);
    expect((await getMonitorDetail(created.task_id)).task.status).toBe("queued");
    await processMatchJob(newJob!);
    const detail = await getMonitorDetail(created.task_id);
    expect(detail.task).toMatchObject({ status: "failed", error: { code: "model_not_configured" } });
    expect(detail.jobs[0].status).toBe("failed");
    const logs = await getTaskLogs(created.task_id, taskLogQuerySchema.parse({}));
    expect(logs.items.some((log) => log.level === "error")).toBe(true);
  });

  test("匹配先按用户、媒体和标签过滤候选，结果持久化真实素材及相似度", async () => {
    const { createMatchTask, processMatchJob } = await import("@/server/services/match");
    const { matchRequestSchema } = await import("@/shared/monitor-contracts");
    const ids = Array.from({ length: 4 }, () => crypto.randomUUID());
    const now = new Date();
    await database.db.insert(schema.assets).values(ids.map((id, index) => ({
      id, userId: index === 1 ? "other-user" : "match-user", name: `素材 ${index}`,
      description: "海边汽车", mediaType: index === 2 ? "image" as const : "video" as const,
      originalFilename: "car.mp4", originalPath: "fixture/car.mp4", mimeType: "video/mp4", sizeBytes: 1,
      processingStatus: "completed" as const, reviewStatus: "published" as const,
      createdAt: now, updatedAt: now,
    })));
    const tagId = crypto.randomUUID();
    await database.db.insert(schema.tags).values({ id: tagId, category: "scene", value: "海边", normalizedValue: "海边", createdAt: now });
    await database.db.insert(schema.assetTags).values(ids.slice(0, 3).map((assetId) => ({ assetId, tagId, source: "human" as const })));
    semanticSearchEnabledMock.mockReturnValue(true);
    searchAnalysisMock.mockResolvedValue(new Map([[ids[0], 0.9], [ids[1], 0.99]]));
    const created = await createMatchTask(matchRequestSchema.parse({ query: "海边汽车", min_score: 0.7,
      filter: { user_scope: { mode: "user", user_id: "match-user" }, media_types: ["video"], tags: [{ category: "scene", value: "海边" }] },
    }));
    await processMatchJob((await repository.claimNextJob())!);
    expect(searchAnalysisMock).toHaveBeenCalledWith("海边汽车", 80, [ids[0]]);
    const result = await new api.DefaultApiV1Service().getTask(created.task_id);
    expect(result).toMatchObject({ status: "done", result: { matched_count: 1, items: [{ asset_id: ids[0], semantic_score: 0.9, user_id: "match-user" }] } });
    expect(JSON.stringify(result.result)).not.toContain(ids[1]);
  });

  async function createAndSeal(filename: string, bytes: Buffer) {
    const service = new api.DefaultApiV1Service();
    const created = await service.createUploadTask({
      user_id: "user-pipeline",
      callback_url: null,
      auto_publish: true,
      items: [{ filename, size_bytes: bytes.byteLength, content_type: null }],
    });
    const itemId = created.items[0]!.item_id;
    const received = await service.receiveUploadItem({
      taskId: created.task_id,
      itemId,
      body: body(bytes),
      contentLength: bytes.byteLength,
      contentType: null,
    });
    expect(received).toMatchObject({
      phase: "waiting_for_seal",
      received_bytes: bytes.byteLength,
    });
    const sealed = await service.sealUploadTask(created.task_id);
    expect(sealed).toMatchObject({ status: "running", phase: "validating" });
    return { service, taskId: created.task_id, itemId };
  }

  async function processUntilIdle(
    sceneClient?: SceneClient,
    videoFramePreparer: Parameters<typeof processing.processJob>[3] = async () => undefined,
  ) {
    while (true) {
      const job = await repository.claimNextJob("pipeline-test");
      if (!job) return;
      await processing.processJob(
        job,
        analyzer,
        undefined,
        videoFramePreparer,
        { storage, sceneClient },
      );
    }
  }

  test("图片三步上传后持久化到对象存储和 MySQL，并完成分析", async () => {
    const image = await sharp({
      create: { width: 8, height: 8, channels: 3, background: "#3388cc" },
    })
      .png()
      .toBuffer();
    const { service, taskId, itemId } = await createAndSeal("sample.png", image);

    await processUntilIdle();
    const status = await service.getTask(taskId);
    expect(status).toMatchObject({
      status: "done",
      phase: "finished",
      progress_percent: 100,
      done_items: 1,
      failed_items: 0,
    });
    expect(status.items[0]).toMatchObject({
      item_id: itemId,
      media_type: "image",
      status: "done",
      phase: "finished",
    });
    expect(status.items[0]!.asset_ids).toHaveLength(1);

    const [assetRow] = await database.db
      .select()
      .from(schema.assets)
      .where(eq(schema.assets.id, status.items[0]!.asset_ids[0]!));
    const [analysis] = await database.db
      .select()
      .from(schema.analysisResults)
      .where(eq(schema.analysisResults.assetId, assetRow!.id));
    const [object] = await database.db
      .select()
      .from(schema.mediaObjects)
      .where(eq(schema.mediaObjects.id, assetRow!.mediaObjectId!));
    expect(assetRow).toMatchObject({
      mediaType: "image",
      processingStatus: "completed",
      reviewStatus: "published",
      userId: "user-pipeline",
    });
    expect(analysis?.resultJson).toMatchObject({
      kind: "image",
      description: "测试图片分析完成",
    });
    expect(object).toMatchObject({ provider: "zos", status: "persisted" });
    expect(storage.objects.get(object!.objectKey)?.bytes).toEqual(image);
    await expect(
      fs.stat(path.join(process.env.MEDIA_ROOT!, ".staging", taskId)),
    ).rejects.toThrow();
  }, 30_000);

  test("对象存储初始化失败时，上传任务与文件同步失败并保留原因", async () => {
    const image = await sharp({ create: { width: 8, height: 8, channels: 3, background: "#3388cc" } }).png().toBuffer();
    const { service, taskId } = await createAndSeal("config-error.png", image);
    const zos = await import("@/server/storage/zos");
    const setup = vi.spyOn(zos, "createZosObjectStorage").mockImplementation(() => { throw new Error("测试：对象存储配置缺失"); });
    try {
      const job = await repository.claimNextJob();
      await processing.processJob(job!, analyzer);
      const status = await service.getTask(taskId);
      expect(status).toMatchObject({ status: "failed", failed_items: 1, progress_percent: 100 });
      expect(status.items[0]).toMatchObject({ status: "failed", error: { message: "测试：对象存储配置缺失" } });
      const { getTaskLogs } = await import("@/server/services/task-monitor");
      const logs = await getTaskLogs(taskId, { after: 0, limit: 100 });
      expect(logs.items.some(log => log.level === "error" && log.message.includes("对象存储配置缺失"))).toBe(true);
    } finally { setup.mockRestore(); }
  });

  test("父视频分镜为多个子素材，父视频不进 assets，每个切片独立关键帧分析", async () => {
    const mediaDirectory = path.join(temporaryRoot, "fixtures");
    await fs.mkdir(mediaDirectory, { recursive: true });
    const paths = ["parent.mp4", "red.mp4", "green.mp4"].map((name) =>
      path.join(mediaDirectory, name),
    );
    await Promise.all([
      createVideo(paths[0]!, "blue", 0.8),
      createVideo(paths[1]!, "red"),
      createVideo(paths[2]!, "green"),
    ]);
    const [parentBytes, ...segments] = await Promise.all(
      paths.map((filePath) => fs.readFile(filePath)),
    );
    const manifest = splitManifest(
      "a".repeat(32),
      segments.map((bytes) => ({ bytes, duration: 0.4 })),
    );
    const fake = fakeSceneClient(manifest, segments);
    const { SceneDetectClient } = await import("@/server/scene/client");
    const client = new SceneDetectClient({
      baseUrl: "http://127.0.0.1:28200",
      timeoutMs: 10_000,
      fetchImplementation: fake.request,
    });
    const { service, taskId, itemId } = await createAndSeal(
      "parent.mp4",
      parentBytes,
    );
    const framePreparation = vi.fn(async () => undefined);

    await processUntilIdle(client, framePreparation);
    const status = await service.getTask(taskId);
    expect(status).toMatchObject({
      status: "done",
      phase: "finished",
      total_items: 1,
      done_items: 1,
      failed_items: 0,
    });
    expect(status.items[0]).toMatchObject({
      item_id: itemId,
      media_type: "video",
      status: "done",
      phase: "finished",
    });
    expect(status.items[0]!.asset_ids).toHaveLength(2);

    const { getTaskLogs } = await import("@/server/services/task-monitor");
    const logPage = await getTaskLogs(taskId, { after: 0, limit: 500 });
    const messages = logPage.items.map(log => log.message);
    for (const step of ["开始校验文件。", "开始视频分镜。", "分镜服务处理完成，生成 2 个片段。", "全部分镜片段准备完成，等待持久化。", "视频批次已完整保存，素材记录和分析作业已提交。", "开始调用模型分析素材。", "上传任务处理结束。"]) {
      expect(messages).toContain(step);
    }
    expect(messages.some(message => message.includes("分析结果已保存"))).toBe(true);
    expect(messages.indexOf("开始视频分镜。")).toBeLessThan(messages.indexOf("分镜服务处理完成，生成 2 个片段。"));
    const { listCalls, callDetail } = await import("@/server/services/call-monitor");
    const calls = await listCalls({ task_id: taskId, limit: 100, page: 1 });
    const validation = calls.items.find(call => call.name === "processJob(validate)")!;
    const validationLogs = await callDetail(validation.id, 0, 500);
    expect(validationLogs.logs.some(log => log.message === "开始视频分镜。")).toBe(true);

    const assetRows = await database.db
      .select()
      .from(schema.assets)
      .where(eq(schema.assets.taskId, taskId));
    const sourceRows = await database.db
      .select()
      .from(schema.videoSources)
      .where(eq(schema.videoSources.taskId, taskId));
    const analysisRows = await database.db.select().from(schema.analysisResults);
    expect(sourceRows).toHaveLength(1);
    expect(assetRows).toHaveLength(2);
    expect(assetRows.map((row) => row.segmentIndex).sort()).toEqual([1, 2]);
    expect(assetRows.every((row) => row.sizeBytes <= 10 * 1024 * 1024)).toBe(true);
    expect(assetRows.every((row) => row.videoSourceId === sourceRows[0]!.id)).toBe(true);
    expect(assetRows.every((row) => row.thumbnailMediaObjectId)).toBe(true);
    expect(assetRows.some((row) => row.id === sourceRows[0]!.id)).toBe(false);
    expect(assetRows.every((row) => row.processingStatus === "completed")).toBe(true);
    expect(analysisRows).toHaveLength(2);
    expect(analysisRows.every((row) => row.resultJson.kind === "video")).toBe(true);
    expect(framePreparation).toHaveBeenCalledTimes(2);
    expect(storage.objects.size).toBe(5); // 1 个父对象 + 2 个切片 + 2 张首帧
    for (const asset of assetRows) {
      const [thumbnail] = await database.db
        .select()
        .from(schema.mediaObjects)
        .where(eq(schema.mediaObjects.id, asset.thumbnailMediaObjectId!));
      expect(thumbnail).toMatchObject({ mimeType: "image/jpeg", status: "persisted" });
      const bytes = storage.objects.get(thumbnail!.objectKey)!.bytes;
      expect([...bytes.subarray(0, 2)]).toEqual([0xff, 0xd8]);
    }
    const { thumbnailResponse } = await import("@/server/media/response");
    const thumbnailGet = await thumbnailResponse(
      assetRows[0]!.id,
      new Request(`http://localhost/api/v1/media/${assetRows[0]!.id}/thumbnail`),
      storage,
    );
    expect(thumbnailGet.status).toBe(200);
    expect(thumbnailGet.headers.get("content-type")).toBe("image/jpeg");
    const thumbnailBytes = new Uint8Array(await thumbnailGet.arrayBuffer());
    expect([...thumbnailBytes.subarray(0, 2)]).toEqual([0xff, 0xd8]);
    const thumbnailRange = await thumbnailResponse(
      assetRows[0]!.id,
      new Request(`http://localhost/api/v1/media/${assetRows[0]!.id}/thumbnail`, {
        headers: { range: "bytes=0-9" },
      }),
      storage,
    );
    expect(thumbnailRange.status).toBe(206);
    expect(thumbnailRange.headers.get("content-range")).toBe(
      `bytes 0-9/${thumbnailBytes.byteLength}`,
    );
    expect((await thumbnailRange.arrayBuffer()).byteLength).toBe(10);
    expect(fake.request).toHaveBeenCalledWith(
      expect.objectContaining({
        pathname: `/api/v1/videos/split/${manifest.taskId}`,
      }),
      expect.objectContaining({ method: "DELETE" }),
    );

    // 用户删除先转公共；公共硬删必须同步回收该切片视频与首帧，兄弟切片和父视频保留。
    const deletedAsset = assetRows[0]!;
    const deletedMainKey = (
      await database.db
        .select()
        .from(schema.mediaObjects)
        .where(eq(schema.mediaObjects.id, deletedAsset.mediaObjectId!))
    )[0]!.objectKey;
    const deletedThumbnailKey = (
      await database.db
        .select()
        .from(schema.mediaObjects)
        .where(eq(schema.mediaObjects.id, deletedAsset.thumbnailMediaObjectId!))
    )[0]!.objectKey;
    await service.deleteAsset(deletedAsset.id, {
      user_id: "user-pipeline",
      callback_url: null,
    });
    await processUntilIdle(client, framePreparation);
    await service.deleteAsset(deletedAsset.id, {
      user_id: null,
      callback_url: null,
    });
    await processUntilIdle(client, framePreparation);
    expect(storage.objects.has(deletedMainKey)).toBe(false);
    expect(storage.objects.has(deletedThumbnailKey)).toBe(false);
    expect(
      await database.db
        .select()
        .from(schema.assets)
        .where(eq(schema.assets.id, deletedAsset.id)),
    ).toHaveLength(0);
    expect(
      await database.db
        .select()
        .from(schema.mediaObjects)
        .where(eq(schema.mediaObjects.id, deletedAsset.thumbnailMediaObjectId!)),
    ).toHaveLength(0);
    expect(storage.objects.size).toBe(3);
  }, 45_000);

  test("任一切片超过 10 MiB 时整批失败，不创建 asset、父视频或媒体对象记录", async () => {
    const parentPath = path.join(temporaryRoot, "oversize-parent.mp4");
    await createVideo(parentPath, "black", 0.4);
    const parentBytes = await fs.readFile(parentPath);
    const limit = 10 * 1024 * 1024;
    const advertised = Buffer.alloc(limit + 1);
    const manifest = splitManifest("b".repeat(32), [
      { bytes: advertised, duration: 0.4 },
    ]);
    const fake = fakeSceneClient(manifest, [advertised]);
    const { SceneDetectClient } = await import("@/server/scene/client");
    const client = new SceneDetectClient({
      baseUrl: "http://127.0.0.1:28200",
      timeoutMs: 10_000,
      fetchImplementation: fake.request,
    });
    const { service, taskId } = await createAndSeal(
      "oversize-parent.mp4",
      parentBytes,
    );

    await processUntilIdle(client);
    const status = await service.getTask(taskId);
    expect(status).toMatchObject({
      status: "failed",
      phase: "finished",
      done_items: 0,
      failed_items: 1,
    });
    expect(status.items[0]).toMatchObject({
      status: "failed",
      phase: "finished",
      asset_ids: [],
      error: {
        code: "segment_too_large",
        details: [
          expect.objectContaining({
            segment_index: 1,
            size_bytes: limit + 1,
            limit_bytes: limit,
          }),
        ],
      },
    });
    expect(await database.db.select().from(schema.assets)).toHaveLength(0);
    expect(await database.db.select().from(schema.videoSources)).toHaveLength(0);
    expect(await database.db.select().from(schema.mediaObjects)).toHaveLength(0);
    expect(storage.objects.size).toBe(0);
    expect(
      fake.request.mock.calls.some(([input]) =>
        String(input).includes("/segments/1"),
      ),
    ).toBe(false);
  }, 30_000);
});
