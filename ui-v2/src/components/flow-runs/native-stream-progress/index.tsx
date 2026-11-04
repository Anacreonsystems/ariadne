import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { useState } from "react";
import {
	buildNativeStreamProjectionQuery,
	type NativeStreamTaskProjection,
} from "@/api/native-streams";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
	Card,
	CardContent,
	CardDescription,
	CardHeader,
	CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
	Table,
	TableBody,
	TableCaption,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import { NativeStreamControlDialog } from "./native-stream-control-dialog";

export function NativeStreamProgress({ flowRunId }: { flowRunId: string }) {
	const [drainOpen, setDrainOpen] = useState(false);
	// This optional view must not suspend the ordinary FlowRun page.
	const { data, isPending, isError, isFetching, refetch } = useQuery(
		buildNativeStreamProjectionQuery(flowRunId),
	);

	return (
		<Card aria-labelledby="native-stream-progress-title">
			<CardHeader>
				<CardTitle>
					<h2 id="native-stream-progress-title">Streaming progress</h2>
				</CardTitle>
				{data?.generation &&
					data.authority_state === "ACTIVE" &&
					!data.source_admission_closed &&
					!data.cancel_requested && (
						<Button
							className="self-start"
							variant="outline"
							size="sm"
							onClick={() => setDrainOpen(true)}
						>
							Drain stream
						</Button>
					)}
				<NativeStreamControlDialog
					flowRunId={flowRunId}
					kind="drain"
					open={drainOpen}
					onOpenChange={setDrainOpen}
				/>
				<CardDescription>
					Durable records and the first unfinished input for each task.
				</CardDescription>
			</CardHeader>
			<CardContent className="space-y-4">
				{isPending ? (
					<div role="status" aria-label="Loading streaming progress">
						<Skeleton className="h-16 w-full" />
					</div>
				) : isError ? (
					<div
						role="alert"
						className="flex flex-wrap items-center justify-between gap-3"
					>
						<p className="text-sm">Streaming progress unavailable</p>
						<Button
							variant="outline"
							size="sm"
							disabled={isFetching}
							onClick={() => void refetch()}
							aria-label="Retry streaming progress"
						>
							Retry
						</Button>
					</div>
				) : data ? (
					<>
						<div className="flex flex-wrap items-center gap-2">
							<Badge variant="outline">
								Generation {data.generation ?? "not claimed"}
							</Badge>
							<Badge variant="secondary">
								{data.authority_state ?? "Awaiting worker"}
							</Badge>
							{data.launch_state && (
								<Badge variant="outline">{data.launch_state}</Badge>
							)}
							{data.source_admission_closed && (
								<Badge variant="outline">Source admission closed</Badge>
							)}
							{data.cancel_requested && (
								<Badge variant="destructive">Cancellation requested</Badge>
							)}
						</div>
						<dl className="flex flex-wrap gap-x-8 gap-y-2 text-sm">
							<div>
								<dt className="text-muted-foreground">Worker</dt>
								<dd className="break-all">
									{data.worker_id ?? "Not assigned"}
								</dd>
							</div>
							<div>
								<dt className="text-muted-foreground">Graph</dt>
								<dd>
									{data.manifest_sealed ? "Sealed" : "Awaiting declarations"}
								</dd>
							</div>
						</dl>
						{data.authority_state === "RELEASED" && (
							<p className="text-sm text-muted-foreground">
								Consumer positions and finite receipts are no longer retained
								for this finished execution. Published positions remain
								available.
							</p>
						)}
						{data.items.length > 0 ? (
							<Table>
								<TableCaption>
									Positions advance after durable acceptance. In-flight input
									may be delivered again.
								</TableCaption>
								<TableHeader>
									<TableRow>
										<TableHead>Task</TableHead>
										<TableHead>State</TableHead>
										<TableHead>Published through</TableHead>
										<TableHead>Completed through</TableHead>
										<TableHead>In-flight input</TableHead>
										<TableHead>Pending inputs</TableHead>
										<TableHead>Stream</TableHead>
										<TableHead>Retained / limit</TableHead>
									</TableRow>
								</TableHeader>
								<TableBody>
									{data.items.map((item) => (
										<StreamingTaskRow
											key={item.task_run_id}
											item={item}
											released={data.authority_state === "RELEASED"}
										/>
									))}
								</TableBody>
							</Table>
						) : (
							<p className="text-sm text-muted-foreground">
								Stream tasks have not been declared yet.
							</p>
						)}
						{data.finite_items.length > 0 && (
							<Table>
								<TableCaption>
									Finite task receipts preserve accepted results across process
									replacement.
								</TableCaption>
								<TableHeader>
									<TableRow>
										<TableHead>Finite task</TableHead>
										<TableHead>Phase</TableHead>
										<TableHead>State</TableHead>
										<TableHead>Durable receipt</TableHead>
									</TableRow>
								</TableHeader>
								<TableBody>
									{data.finite_items.map((item) => (
										<TableRow key={item.task_run_id}>
											<TableCell>
												<Link
													to="/runs/task-run/$id"
													params={{ id: item.task_run_id }}
													className="text-foreground underline underline-offset-4 decoration-muted-foreground hover:decoration-foreground"
												>
													{item.submission_key}
												</Link>
											</TableCell>
											<TableCell>{item.phase}</TableCell>
											<TableCell>
												{item.task_state ?? "Pending"} · attempt{" "}
												{item.run_count}
											</TableCell>
											<TableCell>
												{item.result_committed
													? "Receipt accepted"
													: data.authority_state === "RELEASED"
														? "Receipt no longer retained"
														: "Awaiting receipt"}
											</TableCell>
										</TableRow>
									))}
								</TableBody>
							</Table>
						)}
						<p className="text-xs text-muted-foreground">
							Observed{" "}
							<time dateTime={data.observed_at}>
								{new Date(data.observed_at).toLocaleString()}
							</time>
							. Refreshes every 30 seconds.
						</p>
					</>
				) : null}
			</CardContent>
		</Card>
	);
}

function StreamingTaskRow({
	item,
	released,
}: {
	item: NativeStreamTaskProjection;
	released: boolean;
}) {
	const absentPosition =
		released && item.role === "OPERATOR" ? "Unavailable" : "—";
	return (
		<TableRow>
			<TableCell>
				<Link
					to="/runs/task-run/$id"
					params={{ id: item.task_run_id }}
					className="text-foreground underline underline-offset-4 decoration-muted-foreground hover:decoration-foreground"
				>
					{item.submission_key}
				</Link>
				<div className="text-xs text-muted-foreground">{item.role}</div>
			</TableCell>
			<TableCell>
				{item.task_state ?? "Pending"} · attempt {item.run_count}
			</TableCell>
			<TableCell>{item.last_sequence ?? "—"}</TableCell>
			<TableCell>{item.completed_through ?? absentPosition}</TableCell>
			<TableCell>{item.inflight_sequence ?? absentPosition}</TableCell>
			<TableCell>{item.pending_inputs ?? absentPosition}</TableCell>
			<TableCell>
				{item.stream_state ?? "Awaiting channel"}
				{item.role === "SOURCE" && item.stream_state === "COMPLETED" && (
					<div className="text-xs text-muted-foreground">
						No more source records
					</div>
				)}
			</TableCell>
			<TableCell>
				<div>
					{item.record_count.toLocaleString()} /{" "}
					{item.record_limit.toLocaleString()} records
				</div>
				<div className="text-xs text-muted-foreground">
					{item.byte_count.toLocaleString()} /{" "}
					{item.byte_limit.toLocaleString()} bytes
				</div>
			</TableCell>
		</TableRow>
	);
}
