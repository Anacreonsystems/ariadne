import { QueryClient } from "@tanstack/react-query";
import {
	createMemoryHistory,
	createRootRoute,
	createRouter,
	RouterProvider,
} from "@tanstack/react-router";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { buildApiUrl, createWrapper, server } from "@tests/utils";
import { delay, HttpResponse, http } from "msw";
import { describe, expect, it } from "vitest";
import {
	createFakeNativeStreamProjection,
	createFakeReleasedNativeStreamProjection,
} from "@/mocks/create-fake-native-stream-projection";
import { NativeStreamProgress } from ".";

const endpoint = buildApiUrl("/streams/v1/deployments/:id/projection");

function renderProgress() {
	const route = createRootRoute({
		component: () => <NativeStreamProgress flowRunId="native-run" />,
	});
	const router = createRouter({
		routeTree: route,
		history: createMemoryHistory({ initialEntries: ["/"] }),
		context: { queryClient: new QueryClient() },
	});
	return render(<RouterProvider router={router} />, {
		wrapper: createWrapper(),
	});
}

describe("NativeStreamProgress", () => {
	it("separates durable completion, unfinished input and queued input with TaskRun links", async () => {
		const projection = createFakeNativeStreamProjection();
		server.use(http.get(endpoint, () => HttpResponse.json(projection)));
		renderProgress();
		expect(
			await screen.findByRole("heading", { name: "Streaming progress" }),
		).toBeInTheDocument();
		expect(await screen.findByText("Generation 2")).toBeInTheDocument();
		const taskLink = screen.getByRole("link", { name: "documents.sign" });
		expect(taskLink).toHaveAttribute(
			"href",
			`/runs/task-run/${projection.items[1].task_run_id}`,
		);
		const operatorRow = taskLink.closest("tr");
		expect(operatorRow).not.toBeNull();
		if (!operatorRow) throw new Error("Missing operator row");
		expect(within(operatorRow).getAllByRole("cell")[3]).toHaveTextContent(
			/^199$/,
		);
		expect(within(operatorRow).getByText("200")).toBeInTheDocument();
		expect(within(operatorRow).getByText("301")).toBeInTheDocument();
		expect(screen.getByText("No more source records")).toBeInTheDocument();
		expect(
			screen.getByRole("link", { name: "documents.prepare" }),
		).toHaveAttribute(
			"href",
			`/runs/task-run/${projection.finite_items[0].task_run_id}`,
		);
		expect(screen.getByText("Receipt accepted")).toBeInTheDocument();
		expect(screen.getByText("Awaiting receipt")).toBeInTheDocument();
		expect(screen.getByText(/Observed/)).toBeInTheDocument();
	});

	it("shows loading without hiding the surrounding run page", async () => {
		server.use(
			http.get(endpoint, async () => {
				await delay(100);
				return HttpResponse.json(createFakeNativeStreamProjection());
			}),
		);
		renderProgress();
		expect(
			await screen.findByRole("status", { name: "Loading streaming progress" }),
		).toBeInTheDocument();
		expect(await screen.findByText("Generation 2")).toBeInTheDocument();
	});

	it("shows a failed observation and permits an explicit retry", async () => {
		server.use(
			http.get(endpoint, () =>
				HttpResponse.json({ detail: "Server unavailable" }, { status: 500 }),
			),
		);
		renderProgress();
		expect(await screen.findByRole("alert")).toHaveTextContent(
			"Streaming progress unavailable",
		);
		expect(screen.queryByText("Receipt accepted")).not.toBeInTheDocument();
		server.use(
			http.get(endpoint, () =>
				HttpResponse.json(createFakeNativeStreamProjection()),
			),
		);
		await userEvent.click(
			screen.getByRole("button", { name: "Retry streaming progress" }),
		);
		await waitFor(() =>
			expect(screen.queryByRole("alert")).not.toBeInTheDocument(),
		);
		expect(screen.getByText("Generation 2")).toBeInTheDocument();
	});

	it("drains through the native lifecycle command with the observed generation", async () => {
		let captured: unknown;
		server.use(
			http.get(endpoint, () =>
				HttpResponse.json(createFakeNativeStreamProjection()),
			),
			http.post(
				buildApiUrl("/streams/v1/deployments/control/drain"),
				async ({ request }) => {
					captured = await request.json();
					return HttpResponse.json({
						flow_run_id: "native-run",
						operation_id: "operation",
						generation: 2,
						kind: "DRAIN",
						source_admission_closed: true,
						cancel_requested: false,
						created_at: "2026-11-04T12:00:00Z",
					});
				},
			),
		);
		renderProgress();
		await userEvent.click(
			await screen.findByRole("button", { name: "Drain stream" }),
		);
		expect(
			await screen.findByRole("heading", { name: "Drain streaming flow" }),
		).toBeInTheDocument();
		await userEvent.click(screen.getByRole("button", { name: "Drain flow" }));
		await waitFor(() =>
			expect(captured).toMatchObject({
				flow_run_id: "native-run",
				expected_generation: 2,
				reason: "Requested from the FlowRun page",
			}),
		);
	});

	it("shows cancellation and closed admission separately from stream completion", async () => {
		server.use(
			http.get(endpoint, () =>
				HttpResponse.json(
					createFakeNativeStreamProjection({
						source_admission_closed: true,
						cancel_requested: true,
					}),
				),
			),
		);
		renderProgress();
		expect(
			await screen.findByText("Cancellation requested"),
		).toBeInTheDocument();
		expect(screen.getByText("Source admission closed")).toBeInTheDocument();
		expect(screen.getByText("Awaiting receipt")).toBeInTheDocument();
	});
	it("distinguishes released history from pending work while retaining published positions", async () => {
		server.use(
			http.get(endpoint, () =>
				HttpResponse.json(createFakeReleasedNativeStreamProjection()),
			),
		);
		renderProgress();
		expect(
			await screen.findByText(
				/Consumer positions and finite receipts are no longer retained/,
			),
		).toBeInTheDocument();
		const operatorLink = screen.getByRole("link", { name: "documents.sign" });
		const operatorRow = operatorLink.closest("tr");
		if (!operatorRow) throw new Error("Missing operator row");
		expect(within(operatorRow).getAllByRole("cell")[2]).toHaveTextContent(
			/^500$/,
		);
		expect(within(operatorRow).getAllByRole("cell")[3]).toHaveTextContent(
			"Unavailable",
		);
		expect(screen.queryByText("Awaiting receipt")).not.toBeInTheDocument();
		expect(screen.getAllByText("Receipt no longer retained")).toHaveLength(2);
	});
});
