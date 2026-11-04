import { Container } from "pixi.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { emitter } from "@/graphs/objects/events";
import {
	startFlowRunArtifacts,
	stopFlowRunArtifacts,
} from "@/graphs/objects/flowRunArtifacts";
import {
	startFlowRunEvents,
	stopFlowRunEvents,
} from "@/graphs/objects/flowRunEvents";

const { readApplication, readData, readConfig, createGroup, fetchEvents } =
	vi.hoisted(() => ({
		readApplication: vi.fn(),
		readData: vi.fn(),
		readConfig: vi.fn(),
		createGroup: vi.fn(),
		fetchEvents: vi.fn(),
	}));
vi.mock("@/graphs/objects/application", () => ({
	waitForApplication: readApplication,
}));
vi.mock("@/graphs/objects/nodes", () => ({ waitForRunData: readData }));
vi.mock("@/graphs/objects/config", () => ({ waitForConfig: readConfig }));
vi.mock("@/graphs/objects/settings", () => ({
	waitForSettings: () =>
		Promise.resolve({ disableEvents: false, disableArtifacts: false }),
	layout: { isTemporal: () => true },
}));
vi.mock("@/graphs/factories/runEvents", () => ({
	runEventsFactory: createGroup,
}));
vi.mock("@/graphs/factories/runArtifacts", () => ({
	runArtifactsFactory: createGroup,
}));

const date = new Date("2026-01-01T00:00:00Z");
const data = { start_time: date, end_time: null, artifacts: [] };
const oldEvent = { id: "old", occurred: date };
const newEvent = { id: "new", occurred: date };

function group() {
	return {
		element: new Container(),
		render: vi.fn(() => Promise.resolve()),
		update: vi.fn(),
	};
}

function deferred<T>() {
	let resolve: (value: T) => void = () => {
		throw new Error("Uninitialized deferred");
	};
	let reject: (reason: Error) => void = () => {
		throw new Error("Uninitialized deferred");
	};
	const promise = new Promise<T>((done, fail) => {
		resolve = done;
		reject = fail;
	});
	return { promise, resolve, reject };
}

describe("root graph owner generations", () => {
	const groups: ReturnType<typeof group>[] = [];
	const stages: Container[] = [];
	function scene() {
		const stage = new Container();
		stages.push(stage);
		readApplication.mockResolvedValue({ stage });
		return stage;
	}

	beforeEach(() => {
		vi.useFakeTimers();
		vi.resetAllMocks();
		groups.length = 0;
		stages.length = 0;
		readData.mockResolvedValue(data);
		readConfig.mockResolvedValue({
			runId: "flow",
			fetchEvents,
			fetchEventsInterval: 100,
		});
		fetchEvents.mockResolvedValue([newEvent]);
		createGroup.mockImplementation(() => {
			const next = group();
			groups.push(next);
			return Promise.resolve(next);
		});
	});
	afterEach(() => {
		emitter.clear();
		for (const stage of stages) stage.destroy({ children: true });
		stopFlowRunEvents();
		stopFlowRunArtifacts();
		vi.restoreAllMocks();
		vi.useRealTimers();
	});

	it.each([
		["events", startFlowRunEvents, stopFlowRunEvents],
		["artifacts", startFlowRunArtifacts, stopFlowRunArtifacts],
	] as const)(
		"discards late %s initialization without replacing the new owner",
		async (_name, start, stop) => {
			const oldStage = scene();
			const oldGroup = group();
			const pending = deferred<ReturnType<typeof group>>();
			createGroup.mockReturnValueOnce(pending.promise);
			const oldStart = start();
			await vi.waitFor(() => expect(createGroup).toHaveBeenCalledTimes(1));
			emitter.clear();
			oldStage.destroy({ children: true });
			stop();
			scene();
			await start();
			await vi.advanceTimersByTimeAsync(0);
			pending.resolve(oldGroup);
			await oldStart;
			expect(oldGroup.element.destroyed).toBe(true);
			expect(groups[0].element.destroyed).toBe(false);
			stop();
			expect(groups[0].element.destroyed).toBe(true);
			expect(vi.getTimerCount()).toBe(0);
		},
	);

	it("stops an unattached owner while eventData initialization is suspended", async () => {
		const oldStage = scene();
		const pending = deferred<typeof data>();
		readData.mockResolvedValueOnce(data).mockReturnValueOnce(pending.promise);
		const oldStart = startFlowRunEvents();
		await vi.waitFor(() => expect(readData).toHaveBeenCalledTimes(2));
		expect(oldStage.children).toHaveLength(0);
		emitter.clear();
		oldStage.destroy({ children: true });
		stopFlowRunEvents();
		expect(groups[0].element.destroyed).toBe(true);
		scene();
		await startFlowRunEvents();
		await vi.advanceTimersByTimeAsync(0);
		pending.resolve(data);
		await oldStart;
		expect(groups[1].element.destroyed).toBe(false);
		stopFlowRunEvents();
		expect(groups[1].element.destroyed).toBe(true);
		expect(vi.getTimerCount()).toBe(0);
	});

	it("ignores an old fetch and poller after stop then a new start", async () => {
		const oldStage = scene();
		const pending = deferred<(typeof oldEvent)[]>();
		fetchEvents.mockReturnValueOnce(pending.promise);
		await startFlowRunEvents();
		expect(fetchEvents).toHaveBeenCalledTimes(1);
		emitter.clear();
		oldStage.destroy({ children: true });
		stopFlowRunEvents();
		const newStage = scene();
		await startFlowRunEvents();
		await vi.advanceTimersByTimeAsync(0);
		pending.resolve([oldEvent]);
		await vi.advanceTimersByTimeAsync(0);
		expect(groups[0].render).not.toHaveBeenCalled();
		expect(groups[1].render).toHaveBeenCalledWith([newEvent]);
		expect(newStage.children).toContain(groups[1].element);
		const calls = fetchEvents.mock.calls.length;
		await vi.advanceTimersByTimeAsync(100);
		expect(fetchEvents).toHaveBeenCalledTimes(calls + 1);
		stopFlowRunEvents();
		await vi.advanceTimersByTimeAsync(500);
		expect(fetchEvents).toHaveBeenCalledTimes(calls + 1);
		expect(vi.getTimerCount()).toBe(0);
	});

	it("does not report a rejected fetch from an old stopped scene", async () => {
		const oldStage = scene();
		const pending = deferred<(typeof oldEvent)[]>();
		fetchEvents.mockReturnValueOnce(pending.promise);
		await startFlowRunEvents();
		emitter.clear();
		oldStage.destroy({ children: true });
		stopFlowRunEvents();
		scene();
		await startFlowRunEvents();
		const report = vi.spyOn(console, "error").mockImplementation(() => {});
		pending.reject(new Error("Old scene request failed after stop"));
		await vi.advanceTimersByTimeAsync(0);
		expect(report).not.toHaveBeenCalled();
		expect(groups[1].render).toHaveBeenCalledWith([newEvent]);
	});
});
