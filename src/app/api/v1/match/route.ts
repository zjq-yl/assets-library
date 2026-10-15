import { parseJson, withApiV1 } from "@/server/api/handler";
import { createMatchTask } from "@/server/services/match";
import { matchRequestSchema } from "@/shared/monitor-contracts";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function POST(request: Request) {
  return withApiV1(request, async () => {
    const task = await createMatchTask(await parseJson(request, matchRequestSchema));
    return Response.json(task, {
      status: 202,
      headers: { location: `/api/v1/tasks/${task.task_id}`, "cache-control": "no-store" },
    });
  });
}
