import {
  ChatAttachment,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  OrchestrationCheckpointFile,
  OrchestrationReadModelSummary,
  OrchestrationProposedPlanId,
  OrchestrationReadModel,
  ProjectScript,
  ThreadId,
  TurnId,
  type OrchestrationCheckpointSummary,
  type OrchestrationLatestTurn,
  type OrchestrationMessage,
  type OrchestrationProposedPlan,
  type OrchestrationProject,
  type OrchestrationSession,
  type OrchestrationThread,
  type OrchestrationThreadSummary,
  type OrchestrationThreadActivity,
} from "@t3tools/contracts";
import { Effect, Layer, Schema, Struct } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import {
  isPersistenceError,
  toPersistenceDecodeError,
  toPersistenceSqlError,
  type ProjectionRepositoryError,
} from "../../persistence/Errors.ts";
import { ProjectionCheckpoint } from "../../persistence/Services/ProjectionCheckpoints.ts";
import { ProjectionProject } from "../../persistence/Services/ProjectionProjects.ts";
import { ProjectionState } from "../../persistence/Services/ProjectionState.ts";
import { ProjectionThreadActivity } from "../../persistence/Services/ProjectionThreadActivities.ts";
import { ProjectionThreadMessage } from "../../persistence/Services/ProjectionThreadMessages.ts";
import { ProjectionThreadProposedPlan } from "../../persistence/Services/ProjectionThreadProposedPlans.ts";
import { ProjectionThreadSession } from "../../persistence/Services/ProjectionThreadSessions.ts";
import { ProjectionThread } from "../../persistence/Services/ProjectionThreads.ts";
import { ORCHESTRATION_PROJECTOR_NAMES } from "./ProjectionPipeline.ts";
import {
  ProjectionSnapshotQuery,
  type ProjectionSnapshotQueryShape,
} from "../Services/ProjectionSnapshotQuery.ts";

const decodeReadModel = Schema.decodeUnknownEffect(OrchestrationReadModel);
const decodeReadModelSummary = Schema.decodeUnknownEffect(OrchestrationReadModelSummary);
const ProjectionProjectDbRowSchema = ProjectionProject.mapFields(
  Struct.assign({
    scripts: Schema.fromJsonString(Schema.Array(ProjectScript)),
  }),
);
const ProjectionThreadMessageDbRowSchema = ProjectionThreadMessage.mapFields(
  Struct.assign({
    isStreaming: Schema.Number,
    attachments: Schema.NullOr(Schema.fromJsonString(Schema.Array(ChatAttachment))),
  }),
);
const ProjectionThreadProposedPlanDbRowSchema = ProjectionThreadProposedPlan;
const ProjectionThreadDbRowSchema = ProjectionThread;
const ProjectionThreadActivityDbRowSchema = ProjectionThreadActivity.mapFields(
  Struct.assign({
    payload: Schema.fromJsonString(Schema.Unknown),
    sequence: Schema.NullOr(NonNegativeInt),
  }),
);
const ProjectionThreadSessionDbRowSchema = ProjectionThreadSession;
const ProjectionCheckpointDbRowSchema = ProjectionCheckpoint.mapFields(
  Struct.assign({
    files: Schema.fromJsonString(Schema.Array(OrchestrationCheckpointFile)),
  }),
);
const ProjectionLatestTurnDbRowSchema = Schema.Struct({
  threadId: ProjectionThread.fields.threadId,
  turnId: TurnId,
  state: Schema.String,
  requestedAt: IsoDateTime,
  startedAt: Schema.NullOr(IsoDateTime),
  completedAt: Schema.NullOr(IsoDateTime),
  assistantMessageId: Schema.NullOr(MessageId),
  sourceProposedPlanThreadId: Schema.NullOr(ThreadId),
  sourceProposedPlanId: Schema.NullOr(OrchestrationProposedPlanId),
});
const ProjectionStateDbRowSchema = ProjectionState;

const REQUIRED_SNAPSHOT_PROJECTORS = [
  ORCHESTRATION_PROJECTOR_NAMES.projects,
  ORCHESTRATION_PROJECTOR_NAMES.threads,
  ORCHESTRATION_PROJECTOR_NAMES.threadMessages,
  ORCHESTRATION_PROJECTOR_NAMES.threadProposedPlans,
  ORCHESTRATION_PROJECTOR_NAMES.threadActivities,
  ORCHESTRATION_PROJECTOR_NAMES.threadSessions,
  ORCHESTRATION_PROJECTOR_NAMES.checkpoints,
] as const;

function maxIso(left: string | null, right: string): string {
  if (left === null) {
    return right;
  }
  return left > right ? left : right;
}

function computeSnapshotSequence(
  stateRows: ReadonlyArray<Schema.Schema.Type<typeof ProjectionStateDbRowSchema>>,
): number {
  if (stateRows.length === 0) {
    return 0;
  }
  const sequenceByProjector = new Map(
    stateRows.map((row) => [row.projector, row.lastAppliedSequence] as const),
  );

  let minSequence = Number.POSITIVE_INFINITY;
  for (const projector of REQUIRED_SNAPSHOT_PROJECTORS) {
    const sequence = sequenceByProjector.get(projector);
    if (sequence === undefined) {
      return 0;
    }
    if (sequence < minSequence) {
      minSequence = sequence;
    }
  }

  return Number.isFinite(minSequence) ? minSequence : 0;
}

function toPersistenceSqlOrDecodeError(sqlOperation: string, decodeOperation: string) {
  return (cause: unknown): ProjectionRepositoryError =>
    Schema.isSchemaError(cause)
      ? toPersistenceDecodeError(decodeOperation)(cause)
      : toPersistenceSqlError(sqlOperation)(cause);
}

type ProjectRow = Schema.Schema.Type<typeof ProjectionProjectDbRowSchema>;
type ThreadRow = Schema.Schema.Type<typeof ProjectionThreadDbRowSchema>;
type MessageRow = Schema.Schema.Type<typeof ProjectionThreadMessageDbRowSchema>;
type ProposedPlanRow = Schema.Schema.Type<typeof ProjectionThreadProposedPlanDbRowSchema>;
type ActivityRow = Schema.Schema.Type<typeof ProjectionThreadActivityDbRowSchema>;
type SessionRow = Schema.Schema.Type<typeof ProjectionThreadSessionDbRowSchema>;
type CheckpointRow = Schema.Schema.Type<typeof ProjectionCheckpointDbRowSchema>;
type LatestTurnRow = Schema.Schema.Type<typeof ProjectionLatestTurnDbRowSchema>;
type StateRow = Schema.Schema.Type<typeof ProjectionStateDbRowSchema>;

function mapProjects(projectRows: ReadonlyArray<ProjectRow>): Array<OrchestrationProject> {
  return projectRows.map((row) => ({
    id: row.projectId,
    title: row.title,
    workspaceRoot: row.workspaceRoot,
    defaultModel: row.defaultModel,
    scripts: row.scripts,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    deletedAt: row.deletedAt,
  }));
}

function mapMessages(
  messageRows: ReadonlyArray<MessageRow>,
): Map<string, Array<OrchestrationMessage>> {
  const messagesByThread = new Map<string, Array<OrchestrationMessage>>();
  for (const row of messageRows) {
    const threadMessages = messagesByThread.get(row.threadId) ?? [];
    threadMessages.push({
      id: row.messageId,
      role: row.role,
      text: row.text,
      ...(row.attachments !== null ? { attachments: row.attachments } : {}),
      turnId: row.turnId,
      streaming: row.isStreaming === 1,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    });
    messagesByThread.set(row.threadId, threadMessages);
  }
  return messagesByThread;
}

function mapProposedPlans(
  proposedPlanRows: ReadonlyArray<ProposedPlanRow>,
): Map<string, Array<OrchestrationProposedPlan>> {
  const proposedPlansByThread = new Map<string, Array<OrchestrationProposedPlan>>();
  for (const row of proposedPlanRows) {
    const threadProposedPlans = proposedPlansByThread.get(row.threadId) ?? [];
    threadProposedPlans.push({
      id: row.planId,
      turnId: row.turnId,
      planMarkdown: row.planMarkdown,
      implementedAt: row.implementedAt,
      implementationThreadId: row.implementationThreadId,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    });
    proposedPlansByThread.set(row.threadId, threadProposedPlans);
  }
  return proposedPlansByThread;
}

function mapActivities(
  activityRows: ReadonlyArray<ActivityRow>,
): Map<string, Array<OrchestrationThreadActivity>> {
  const activitiesByThread = new Map<string, Array<OrchestrationThreadActivity>>();
  for (const row of activityRows) {
    const threadActivities = activitiesByThread.get(row.threadId) ?? [];
    threadActivities.push({
      id: row.activityId,
      tone: row.tone,
      kind: row.kind,
      summary: row.summary,
      payload: row.payload,
      turnId: row.turnId,
      ...(row.sequence !== null ? { sequence: row.sequence } : {}),
      createdAt: row.createdAt,
    });
    activitiesByThread.set(row.threadId, threadActivities);
  }
  return activitiesByThread;
}

function mapCheckpoints(
  checkpointRows: ReadonlyArray<CheckpointRow>,
): Map<string, Array<OrchestrationCheckpointSummary>> {
  const checkpointsByThread = new Map<string, Array<OrchestrationCheckpointSummary>>();
  for (const row of checkpointRows) {
    const threadCheckpoints = checkpointsByThread.get(row.threadId) ?? [];
    threadCheckpoints.push({
      turnId: row.turnId,
      checkpointTurnCount: row.checkpointTurnCount,
      checkpointRef: row.checkpointRef,
      status: row.status,
      files: row.files,
      assistantMessageId: row.assistantMessageId,
      completedAt: row.completedAt,
    });
    checkpointsByThread.set(row.threadId, threadCheckpoints);
  }
  return checkpointsByThread;
}

function mapLatestTurns(
  latestTurnRows: ReadonlyArray<LatestTurnRow>,
): Map<string, OrchestrationLatestTurn> {
  const latestTurnByThread = new Map<string, OrchestrationLatestTurn>();
  for (const row of latestTurnRows) {
    if (latestTurnByThread.has(row.threadId)) {
      continue;
    }
    latestTurnByThread.set(row.threadId, {
      turnId: row.turnId,
      state:
        row.state === "error"
          ? "error"
          : row.state === "interrupted"
            ? "interrupted"
            : row.state === "completed"
              ? "completed"
              : "running",
      requestedAt: row.requestedAt,
      startedAt: row.startedAt,
      completedAt: row.completedAt,
      assistantMessageId: row.assistantMessageId,
      ...(row.sourceProposedPlanThreadId !== null && row.sourceProposedPlanId !== null
        ? {
            sourceProposedPlan: {
              threadId: row.sourceProposedPlanThreadId,
              planId: row.sourceProposedPlanId,
            },
          }
        : {}),
    });
  }
  return latestTurnByThread;
}

function mapSessions(sessionRows: ReadonlyArray<SessionRow>): Map<string, OrchestrationSession> {
  const sessionsByThread = new Map<string, OrchestrationSession>();
  for (const row of sessionRows) {
    sessionsByThread.set(row.threadId, {
      threadId: row.threadId,
      status: row.status,
      providerName: row.providerName,
      runtimeMode: row.runtimeMode,
      activeTurnId: row.activeTurnId,
      lastError: row.lastError,
      updatedAt: row.updatedAt,
    });
  }
  return sessionsByThread;
}

function computeUpdatedAt(input: {
  readonly projectRows: ReadonlyArray<ProjectRow>;
  readonly threadRows: ReadonlyArray<ThreadRow>;
  readonly stateRows: ReadonlyArray<StateRow>;
  readonly messageRows?: ReadonlyArray<MessageRow>;
  readonly proposedPlanRows?: ReadonlyArray<ProposedPlanRow>;
  readonly activityRows?: ReadonlyArray<ActivityRow>;
  readonly sessionRows?: ReadonlyArray<SessionRow>;
  readonly checkpointRows?: ReadonlyArray<CheckpointRow>;
  readonly latestTurnRows?: ReadonlyArray<LatestTurnRow>;
}): string {
  let updatedAt: string | null = null;

  for (const row of input.projectRows) updatedAt = maxIso(updatedAt, row.updatedAt);
  for (const row of input.threadRows) updatedAt = maxIso(updatedAt, row.updatedAt);
  for (const row of input.stateRows) updatedAt = maxIso(updatedAt, row.updatedAt);
  for (const row of input.messageRows ?? []) updatedAt = maxIso(updatedAt, row.updatedAt);
  for (const row of input.proposedPlanRows ?? []) updatedAt = maxIso(updatedAt, row.updatedAt);
  for (const row of input.activityRows ?? []) updatedAt = maxIso(updatedAt, row.createdAt);
  for (const row of input.sessionRows ?? []) updatedAt = maxIso(updatedAt, row.updatedAt);
  for (const row of input.checkpointRows ?? []) updatedAt = maxIso(updatedAt, row.completedAt);
  for (const row of input.latestTurnRows ?? []) {
    updatedAt = maxIso(updatedAt, row.requestedAt);
    if (row.startedAt !== null) updatedAt = maxIso(updatedAt, row.startedAt);
    if (row.completedAt !== null) updatedAt = maxIso(updatedAt, row.completedAt);
  }

  return updatedAt ?? new Date(0).toISOString();
}

function buildThreadSummaries(input: {
  readonly threadRows: ReadonlyArray<ThreadRow>;
  readonly proposedPlansByThread: ReadonlyMap<string, Array<OrchestrationProposedPlan>>;
  readonly activitiesByThread: ReadonlyMap<string, Array<OrchestrationThreadActivity>>;
  readonly sessionsByThread: ReadonlyMap<string, OrchestrationSession>;
  readonly latestTurnByThread: ReadonlyMap<string, OrchestrationLatestTurn>;
}): Array<OrchestrationThreadSummary> {
  return input.threadRows.map((row) => ({
    id: row.threadId,
    projectId: row.projectId,
    title: row.title,
    model: row.model,
    runtimeMode: row.runtimeMode,
    interactionMode: row.interactionMode,
    branch: row.branch,
    worktreePath: row.worktreePath,
    latestTurn: input.latestTurnByThread.get(row.threadId) ?? null,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    deletedAt: row.deletedAt,
    proposedPlans: input.proposedPlansByThread.get(row.threadId) ?? [],
    activities: input.activitiesByThread.get(row.threadId) ?? [],
    session: input.sessionsByThread.get(row.threadId) ?? null,
  }));
}

function buildThreads(input: {
  readonly threadRows: ReadonlyArray<ThreadRow>;
  readonly messagesByThread: ReadonlyMap<string, Array<OrchestrationMessage>>;
  readonly proposedPlansByThread: ReadonlyMap<string, Array<OrchestrationProposedPlan>>;
  readonly activitiesByThread: ReadonlyMap<string, Array<OrchestrationThreadActivity>>;
  readonly checkpointsByThread: ReadonlyMap<string, Array<OrchestrationCheckpointSummary>>;
  readonly sessionsByThread: ReadonlyMap<string, OrchestrationSession>;
  readonly latestTurnByThread: ReadonlyMap<string, OrchestrationLatestTurn>;
}): Array<OrchestrationThread> {
  return input.threadRows.map((row) => ({
    id: row.threadId,
    projectId: row.projectId,
    title: row.title,
    model: row.model,
    runtimeMode: row.runtimeMode,
    interactionMode: row.interactionMode,
    branch: row.branch,
    worktreePath: row.worktreePath,
    latestTurn: input.latestTurnByThread.get(row.threadId) ?? null,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    deletedAt: row.deletedAt,
    messages: input.messagesByThread.get(row.threadId) ?? [],
    proposedPlans: input.proposedPlansByThread.get(row.threadId) ?? [],
    activities: input.activitiesByThread.get(row.threadId) ?? [],
    checkpoints: input.checkpointsByThread.get(row.threadId) ?? [],
    session: input.sessionsByThread.get(row.threadId) ?? null,
  }));
}

const makeProjectionSnapshotQuery = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const listProjectRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionProjectDbRowSchema,
    execute: () =>
      sql`
        SELECT
          project_id AS "projectId",
          title,
          workspace_root AS "workspaceRoot",
          default_model AS "defaultModel",
          scripts_json AS "scripts",
          created_at AS "createdAt",
          updated_at AS "updatedAt",
          deleted_at AS "deletedAt"
        FROM projection_projects
        ORDER BY created_at ASC, project_id ASC
      `,
  });

  const listThreadRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadDbRowSchema,
    execute: () =>
      sql`
        SELECT
          thread_id AS "threadId",
          project_id AS "projectId",
          title,
          model,
          runtime_mode AS "runtimeMode",
          interaction_mode AS "interactionMode",
          branch,
          worktree_path AS "worktreePath",
          latest_turn_id AS "latestTurnId",
          created_at AS "createdAt",
          updated_at AS "updatedAt",
          deleted_at AS "deletedAt"
        FROM projection_threads
        ORDER BY created_at ASC, thread_id ASC
      `,
  });

  const listThreadMessageRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadMessageDbRowSchema,
    execute: () =>
      sql`
        SELECT
          message_id AS "messageId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          role,
          text,
          attachments_json AS "attachments",
          is_streaming AS "isStreaming",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM projection_thread_messages
        ORDER BY thread_id ASC, created_at ASC, message_id ASC
      `,
  });

  const listThreadProposedPlanRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadProposedPlanDbRowSchema,
    execute: () =>
      sql`
        SELECT
          plan_id AS "planId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          plan_markdown AS "planMarkdown",
          implemented_at AS "implementedAt",
          implementation_thread_id AS "implementationThreadId",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM projection_thread_proposed_plans
        ORDER BY thread_id ASC, created_at ASC, plan_id ASC
      `,
  });

  const listThreadActivityRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadActivityDbRowSchema,
    execute: () =>
      sql`
        SELECT
          activity_id AS "activityId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          tone,
          kind,
          summary,
          payload_json AS "payload",
          sequence,
          created_at AS "createdAt"
        FROM projection_thread_activities
        ORDER BY
          thread_id ASC,
          CASE WHEN sequence IS NULL THEN 0 ELSE 1 END ASC,
          sequence ASC,
          created_at ASC,
          activity_id ASC
      `,
  });

  const listThreadSessionRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionThreadSessionDbRowSchema,
    execute: () =>
      sql`
        SELECT
          thread_id AS "threadId",
          status,
          provider_name AS "providerName",
          provider_session_id AS "providerSessionId",
          provider_thread_id AS "providerThreadId",
          runtime_mode AS "runtimeMode",
          active_turn_id AS "activeTurnId",
          last_error AS "lastError",
          updated_at AS "updatedAt"
        FROM projection_thread_sessions
        ORDER BY thread_id ASC
      `,
  });

  const listCheckpointRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionCheckpointDbRowSchema,
    execute: () =>
      sql`
        SELECT
          thread_id AS "threadId",
          turn_id AS "turnId",
          checkpoint_turn_count AS "checkpointTurnCount",
          checkpoint_ref AS "checkpointRef",
          checkpoint_status AS "status",
          checkpoint_files_json AS "files",
          assistant_message_id AS "assistantMessageId",
          completed_at AS "completedAt"
        FROM projection_turns
        WHERE checkpoint_turn_count IS NOT NULL
        ORDER BY thread_id ASC, checkpoint_turn_count ASC
      `,
  });

  const listLatestTurnRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionLatestTurnDbRowSchema,
    execute: () =>
      sql`
        SELECT
          thread_id AS "threadId",
          turn_id AS "turnId",
          state,
          requested_at AS "requestedAt",
          started_at AS "startedAt",
          completed_at AS "completedAt",
          assistant_message_id AS "assistantMessageId",
          source_proposed_plan_thread_id AS "sourceProposedPlanThreadId",
          source_proposed_plan_id AS "sourceProposedPlanId"
        FROM projection_turns
        WHERE turn_id IS NOT NULL
        ORDER BY thread_id ASC, requested_at DESC, turn_id DESC
      `,
  });

  const listProjectionStateRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProjectionStateDbRowSchema,
    execute: () =>
      sql`
        SELECT
          projector,
          last_applied_sequence AS "lastAppliedSequence",
          updated_at AS "updatedAt"
        FROM projection_state
      `,
  });

  const threadRequest = Schema.Struct({ threadId: ThreadId });

  const getThreadRow = SqlSchema.findOne({
    Request: threadRequest,
    Result: ProjectionThreadDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          project_id AS "projectId",
          title,
          model,
          runtime_mode AS "runtimeMode",
          interaction_mode AS "interactionMode",
          branch,
          worktree_path AS "worktreePath",
          latest_turn_id AS "latestTurnId",
          created_at AS "createdAt",
          updated_at AS "updatedAt",
          deleted_at AS "deletedAt"
        FROM projection_threads
        WHERE thread_id = ${threadId}
      `,
  });

  const listThreadMessageRowsByThread = SqlSchema.findAll({
    Request: threadRequest,
    Result: ProjectionThreadMessageDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          message_id AS "messageId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          role,
          text,
          attachments_json AS "attachments",
          is_streaming AS "isStreaming",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM projection_thread_messages
        WHERE thread_id = ${threadId}
        ORDER BY created_at ASC, message_id ASC
      `,
  });

  const listThreadProposedPlanRowsByThread = SqlSchema.findAll({
    Request: threadRequest,
    Result: ProjectionThreadProposedPlanDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          plan_id AS "planId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          plan_markdown AS "planMarkdown",
          implemented_at AS "implementedAt",
          implementation_thread_id AS "implementationThreadId",
          created_at AS "createdAt",
          updated_at AS "updatedAt"
        FROM projection_thread_proposed_plans
        WHERE thread_id = ${threadId}
        ORDER BY created_at ASC, plan_id ASC
      `,
  });

  const listThreadActivityRowsByThread = SqlSchema.findAll({
    Request: threadRequest,
    Result: ProjectionThreadActivityDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          activity_id AS "activityId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          tone,
          kind,
          summary,
          payload_json AS "payload",
          sequence,
          created_at AS "createdAt"
        FROM projection_thread_activities
        WHERE thread_id = ${threadId}
        ORDER BY
          CASE WHEN sequence IS NULL THEN 0 ELSE 1 END ASC,
          sequence ASC,
          created_at ASC,
          activity_id ASC
      `,
  });

  const getThreadSessionRow = SqlSchema.findOne({
    Request: threadRequest,
    Result: ProjectionThreadSessionDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          status,
          provider_name AS "providerName",
          provider_session_id AS "providerSessionId",
          provider_thread_id AS "providerThreadId",
          runtime_mode AS "runtimeMode",
          active_turn_id AS "activeTurnId",
          last_error AS "lastError",
          updated_at AS "updatedAt"
        FROM projection_thread_sessions
        WHERE thread_id = ${threadId}
      `,
  });

  const listCheckpointRowsByThread = SqlSchema.findAll({
    Request: threadRequest,
    Result: ProjectionCheckpointDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          turn_id AS "turnId",
          checkpoint_turn_count AS "checkpointTurnCount",
          checkpoint_ref AS "checkpointRef",
          checkpoint_status AS "status",
          checkpoint_files_json AS "files",
          assistant_message_id AS "assistantMessageId",
          completed_at AS "completedAt"
        FROM projection_turns
        WHERE thread_id = ${threadId} AND checkpoint_turn_count IS NOT NULL
        ORDER BY checkpoint_turn_count ASC
      `,
  });

  const listLatestTurnRowsByThread = SqlSchema.findAll({
    Request: threadRequest,
    Result: ProjectionLatestTurnDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          thread_id AS "threadId",
          turn_id AS "turnId",
          state,
          requested_at AS "requestedAt",
          started_at AS "startedAt",
          completed_at AS "completedAt",
          assistant_message_id AS "assistantMessageId",
          source_proposed_plan_thread_id AS "sourceProposedPlanThreadId",
          source_proposed_plan_id AS "sourceProposedPlanId"
        FROM projection_turns
        WHERE thread_id = ${threadId} AND turn_id IS NOT NULL
        ORDER BY requested_at DESC, turn_id DESC
      `,
  });

  const getSnapshotSummary: ProjectionSnapshotQueryShape["getSnapshotSummary"] = () =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const [
            projectRows,
            threadRows,
            proposedPlanRows,
            sessionRows,
            latestTurnRows,
            stateRows,
          ] = yield* Effect.all([
            listProjectRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshotSummary:listProjects:query",
                  "ProjectionSnapshotQuery.getSnapshotSummary:listProjects:decodeRows",
                ),
              ),
            ),
            listThreadRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshotSummary:listThreads:query",
                  "ProjectionSnapshotQuery.getSnapshotSummary:listThreads:decodeRows",
                ),
              ),
            ),
            listThreadProposedPlanRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshotSummary:listThreadProposedPlans:query",
                  "ProjectionSnapshotQuery.getSnapshotSummary:listThreadProposedPlans:decodeRows",
                ),
              ),
            ),
            listThreadSessionRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshotSummary:listThreadSessions:query",
                  "ProjectionSnapshotQuery.getSnapshotSummary:listThreadSessions:decodeRows",
                ),
              ),
            ),
            listLatestTurnRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshotSummary:listLatestTurns:query",
                  "ProjectionSnapshotQuery.getSnapshotSummary:listLatestTurns:decodeRows",
                ),
              ),
            ),
            listProjectionStateRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshotSummary:listProjectionState:query",
                  "ProjectionSnapshotQuery.getSnapshotSummary:listProjectionState:decodeRows",
                ),
              ),
            ),
          ]);

          const snapshot = {
            snapshotSequence: computeSnapshotSequence(stateRows),
            projects: mapProjects(projectRows),
            threads: buildThreadSummaries({
              threadRows,
              proposedPlansByThread: mapProposedPlans(proposedPlanRows),
              activitiesByThread: new Map(),
              sessionsByThread: mapSessions(sessionRows),
              latestTurnByThread: mapLatestTurns(latestTurnRows),
            }),
            updatedAt: computeUpdatedAt({
              projectRows,
              threadRows,
              stateRows,
              proposedPlanRows,
              sessionRows,
              latestTurnRows,
            }),
          };

          return yield* decodeReadModelSummary(snapshot).pipe(
            Effect.mapError(
              toPersistenceDecodeError(
                "ProjectionSnapshotQuery.getSnapshotSummary:decodeReadModel",
              ),
            ),
          );
        }),
      )
      .pipe(
        Effect.mapError((error) => {
          if (isPersistenceError(error)) {
            return error;
          }
          return toPersistenceSqlError("ProjectionSnapshotQuery.getSnapshotSummary:query")(error);
        }),
      );

  const getThreadSnapshot: ProjectionSnapshotQueryShape["getThreadSnapshot"] = (threadId) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const [
            threadRow,
            messageRows,
            proposedPlanRows,
            activityRows,
            sessionRow,
            checkpointRows,
            latestTurnRows,
          ] = yield* Effect.all([
            getThreadRow({ threadId }).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getThreadSnapshot:getThread:query",
                  "ProjectionSnapshotQuery.getThreadSnapshot:getThread:decodeRows",
                ),
              ),
            ),
            listThreadMessageRowsByThread({ threadId }).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getThreadSnapshot:listThreadMessages:query",
                  "ProjectionSnapshotQuery.getThreadSnapshot:listThreadMessages:decodeRows",
                ),
              ),
            ),
            listThreadProposedPlanRowsByThread({ threadId }).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getThreadSnapshot:listThreadProposedPlans:query",
                  "ProjectionSnapshotQuery.getThreadSnapshot:listThreadProposedPlans:decodeRows",
                ),
              ),
            ),
            listThreadActivityRowsByThread({ threadId }).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getThreadSnapshot:listThreadActivities:query",
                  "ProjectionSnapshotQuery.getThreadSnapshot:listThreadActivities:decodeRows",
                ),
              ),
            ),
            getThreadSessionRow({ threadId }).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getThreadSnapshot:getThreadSession:query",
                  "ProjectionSnapshotQuery.getThreadSnapshot:getThreadSession:decodeRows",
                ),
              ),
            ),
            listCheckpointRowsByThread({ threadId }).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getThreadSnapshot:listCheckpoints:query",
                  "ProjectionSnapshotQuery.getThreadSnapshot:listCheckpoints:decodeRows",
                ),
              ),
            ),
            listLatestTurnRowsByThread({ threadId }).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getThreadSnapshot:listLatestTurns:query",
                  "ProjectionSnapshotQuery.getThreadSnapshot:listLatestTurns:decodeRows",
                ),
              ),
            ),
          ]);

          if (threadRow === null || threadRow.deletedAt !== null) {
            return null;
          }

          const threads = buildThreads({
            threadRows: [threadRow],
            messagesByThread: mapMessages(messageRows),
            proposedPlansByThread: mapProposedPlans(proposedPlanRows),
            activitiesByThread: mapActivities(activityRows),
            checkpointsByThread: mapCheckpoints(checkpointRows),
            sessionsByThread: mapSessions(sessionRow ? [sessionRow] : []),
            latestTurnByThread: mapLatestTurns(latestTurnRows),
          });

          return threads[0] ?? null;
        }),
      )
      .pipe(
        Effect.mapError((error) => {
          if (isPersistenceError(error)) {
            return error;
          }
          return toPersistenceSqlError("ProjectionSnapshotQuery.getThreadSnapshot:query")(error);
        }),
      );

  const getSnapshot: ProjectionSnapshotQueryShape["getSnapshot"] = () =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const [
            projectRows,
            threadRows,
            messageRows,
            proposedPlanRows,
            activityRows,
            sessionRows,
            checkpointRows,
            latestTurnRows,
            stateRows,
          ] = yield* Effect.all([
            listProjectRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listProjects:query",
                  "ProjectionSnapshotQuery.getSnapshot:listProjects:decodeRows",
                ),
              ),
            ),
            listThreadRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listThreads:query",
                  "ProjectionSnapshotQuery.getSnapshot:listThreads:decodeRows",
                ),
              ),
            ),
            listThreadMessageRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listThreadMessages:query",
                  "ProjectionSnapshotQuery.getSnapshot:listThreadMessages:decodeRows",
                ),
              ),
            ),
            listThreadProposedPlanRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listThreadProposedPlans:query",
                  "ProjectionSnapshotQuery.getSnapshot:listThreadProposedPlans:decodeRows",
                ),
              ),
            ),
            listThreadActivityRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listThreadActivities:query",
                  "ProjectionSnapshotQuery.getSnapshot:listThreadActivities:decodeRows",
                ),
              ),
            ),
            listThreadSessionRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listThreadSessions:query",
                  "ProjectionSnapshotQuery.getSnapshot:listThreadSessions:decodeRows",
                ),
              ),
            ),
            listCheckpointRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listCheckpoints:query",
                  "ProjectionSnapshotQuery.getSnapshot:listCheckpoints:decodeRows",
                ),
              ),
            ),
            listLatestTurnRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listLatestTurns:query",
                  "ProjectionSnapshotQuery.getSnapshot:listLatestTurns:decodeRows",
                ),
              ),
            ),
            listProjectionStateRows(undefined).pipe(
              Effect.mapError(
                toPersistenceSqlOrDecodeError(
                  "ProjectionSnapshotQuery.getSnapshot:listProjectionState:query",
                  "ProjectionSnapshotQuery.getSnapshot:listProjectionState:decodeRows",
                ),
              ),
            ),
          ]);

          const snapshot = {
            snapshotSequence: computeSnapshotSequence(stateRows),
            projects: mapProjects(projectRows),
            threads: buildThreads({
              threadRows,
              messagesByThread: mapMessages(messageRows),
              proposedPlansByThread: mapProposedPlans(proposedPlanRows),
              activitiesByThread: mapActivities(activityRows),
              checkpointsByThread: mapCheckpoints(checkpointRows),
              sessionsByThread: mapSessions(sessionRows),
              latestTurnByThread: mapLatestTurns(latestTurnRows),
            }),
            updatedAt: computeUpdatedAt({
              projectRows,
              threadRows,
              stateRows,
              messageRows,
              proposedPlanRows,
              activityRows,
              sessionRows,
              checkpointRows,
              latestTurnRows,
            }),
          };

          return yield* decodeReadModel(snapshot).pipe(
            Effect.mapError(
              toPersistenceDecodeError("ProjectionSnapshotQuery.getSnapshot:decodeReadModel"),
            ),
          );
        }),
      )
      .pipe(
        Effect.mapError((error) => {
          if (isPersistenceError(error)) {
            return error;
          }
          return toPersistenceSqlError("ProjectionSnapshotQuery.getSnapshot:query")(error);
        }),
      );

  return {
    getSnapshot,
    getSnapshotSummary,
    getThreadSnapshot,
  } satisfies ProjectionSnapshotQueryShape;
});

export const OrchestrationProjectionSnapshotQueryLive = Layer.effect(
  ProjectionSnapshotQuery,
  makeProjectionSnapshotQuery,
);
