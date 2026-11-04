import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { toast } from "sonner";
import {
	buildNativeStreamProjectionQuery,
	type NativeStreamControlKind,
	useNativeStreamControl,
} from "@/api/native-streams";
import {
	AlertDialog,
	AlertDialogAction,
	AlertDialogCancel,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";

export function NativeStreamControlDialog({
	flowRunId,
	kind,
	open,
	onOpenChange,
	beforeEnrollmentContent,
}: {
	flowRunId: string;
	kind: NativeStreamControlKind;
	open: boolean;
	onOpenChange: (open: boolean) => void;
	beforeEnrollmentContent?: ReactNode;
}) {
	const { data, isPending, isError, isFetching, refetch } = useQuery({
		...buildNativeStreamProjectionQuery(flowRunId),
		enabled: open,
	});
	const {
		requestControl,
		isPending: isSending,
		isError: requestFailed,
	} = useNativeStreamControl();
	const isCancel = kind === "cancel";
	if (
		data &&
		data.generation === null &&
		!isError &&
		!isFetching &&
		beforeEnrollmentContent
	)
		return beforeEnrollmentContent;
	const alreadyRequested = isCancel
		? data?.cancel_requested
		: data?.source_admission_closed;
	const disabled =
		isPending ||
		isError ||
		isFetching ||
		isSending ||
		!data?.generation ||
		alreadyRequested ||
		data?.authority_state === "TERMINAL" ||
		data?.authority_state === "RELEASED";

	return (
		<AlertDialog open={open} onOpenChange={onOpenChange}>
			<AlertDialogContent>
				<AlertDialogHeader>
					<AlertDialogTitle>
						{isCancel ? "Cancel Flow Run" : "Drain streaming flow"}
					</AlertDialogTitle>
					<AlertDialogDescription>
						{isCancel
							? "Stop this flow and reject further stream writes. Cancellation completes after the worker confirms that its process has stopped."
							: "Stop accepting new source records. Accepted work continues through the stream and the finite tail before the flow completes."}
					</AlertDialogDescription>
				</AlertDialogHeader>
				{isPending ? (
					<p role="status">Loading current streaming generation…</p>
				) : isError ? (
					<div role="alert">
						<p>Current streaming generation unavailable.</p>
						<Button variant="outline" onClick={() => void refetch()}>
							Refresh generation
						</Button>
					</div>
				) : data?.generation ? (
					<p className="text-sm">
						Generation {data.generation}
						{alreadyRequested ? " · request already accepted" : ""}
					</p>
				) : (
					<p className="text-sm">Awaiting worker enrollment.</p>
				)}
				{requestFailed && (
					<p role="alert" className="text-sm">
						The server has not confirmed this request. You can retry.
					</p>
				)}
				<AlertDialogFooter>
					<AlertDialogCancel disabled={isSending}>
						Keep running
					</AlertDialogCancel>
					<AlertDialogAction
						disabled={disabled}
						onClick={(event) => {
							event.preventDefault();
							if (!data?.generation) return;
							requestControl(
								{ flowRunId, expectedGeneration: data.generation, kind },
								{
									onSuccess: () => {
										toast.success(
											isCancel ? "Cancellation requested" : "Drain requested",
										);
										onOpenChange(false);
									},
									onError: () =>
										toast.error("The streaming request was not confirmed"),
								},
							);
						}}
					>
						{isSending
							? "Sending request…"
							: isCancel
								? "Cancel Flow Run"
								: "Drain flow"}
					</AlertDialogAction>
				</AlertDialogFooter>
			</AlertDialogContent>
		</AlertDialog>
	);
}
