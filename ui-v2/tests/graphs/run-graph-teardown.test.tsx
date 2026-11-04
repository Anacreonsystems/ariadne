import { Container, Rectangle } from "pixi.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_ROOT_COLLISION_THROTTLE } from "@/graphs/consts";
import { runArtifactsFactory } from "@/graphs/factories/runArtifacts";
import { runEventsFactory } from "@/graphs/factories/runEvents";
import type { RunGraphArtifact, RunGraphEvent } from "@/graphs/models";
import { emitter } from "@/graphs/objects/events";
import * as collisions from "@/graphs/utilities/detectHorizontalCollisions";

const { createLeaf } = vi.hoisted(() => ({ createLeaf: vi.fn() }));

vi.mock("@/graphs/objects/settings", () => ({
	waitForSettings: () =>
		Promise.resolve({
			disableEvents: false,
			disableArtifacts: false,
		}),
	layout: { isTemporal: () => true },
}));
vi.mock("@/graphs/factories/flowRunEvent", () => ({
	flowRunEventFactory: createLeaf,
}));
vi.mock("@/graphs/factories/nodeFlowRunEvent", () => ({
	nodeFlowRunEventFactory: createLeaf,
}));
vi.mock("@/graphs/factories/flowRunArtifact", () => ({
	flowRunArtifactFactory: createLeaf,
}));
vi.mock("@/graphs/factories/nodeFlowRunArtifact", () => ({
	nodeFlowRunArtifactFactory: createLeaf,
}));

type Datum = RunGraphEvent & RunGraphArtifact;
type LeafOptions = { type: string; event?: Datum; artifact?: Datum };

function datum(id: string): Datum {
	return {
		id,
		created: new Date("2026-01-01T00:00:00Z"),
		occurred: new Date("2026-01-01T00:00:00Z"),
		received: new Date("2026-01-01T00:00:00Z"),
		type: "result",
		event: "prefect.task-run.Completed",
		payload: null,
		related: [],
		resource: { "prefect.resource.id": id },
	};
}

function leaf(options: LeafOptions) {
	const item = options.event ?? options.artifact;
	const element = new Container({ boundsArea: new Rectangle(0, 0, 20, 10) });
	element.x = item?.id === "second" ? 5 : 0;
	let ids: string[] = [];
	let date: Date | null = item?.occurred ?? null;
	const render = vi.fn((props?: { ids: string[]; date: Date }) => {
		if (props?.ids) {
			ids = props.ids;
			date = props.date;
		}
		return Promise.resolve();
	});
	return {
		element,
		render,
		getDate: () => date,
		...(item ? { getId: () => item.id } : { getIds: () => ids }),
	};
}

function deferred<T>() {
	let resolve: (value: T) => void = () => {
		throw new Error("Deferred promise has not been initialized");
	};
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

describe.each([
	["events", runEventsFactory],
	["artifacts", runArtifactsFactory],
] as const)("%s graph owner teardown", (_name, factory) => {
	beforeEach(() => {
		vi.useFakeTimers();
		createLeaf.mockReset();
		createLeaf.mockImplementation((options: LeafOptions) =>
			Promise.resolve(leaf(options)),
		);
		vi.spyOn(collisions, "clusterHorizontalCollisions");
	});

	afterEach(() => {
		emitter.clear();
		vi.restoreAllMocks();
		vi.useRealTimers();
	});

	it("keeps the trailing layout while the Pixi owner is alive", async () => {
		const group = await factory({ isRoot: true });
		await group.render([datum("first")]);
		group.update();
		await vi.advanceTimersByTimeAsync(DEFAULT_ROOT_COLLISION_THROTTLE);
		expect(collisions.clusterHorizontalCollisions).toHaveBeenCalledTimes(2);
		expect(group.element.children).toHaveLength(1);
		group.element.destroy({ children: true });
	});

	it("cancels the queued layout before recursive Pixi child destruction", async () => {
		const group = await factory({ isRoot: true });
		await group.render([datum("first"), datum("second")]);
		await vi.advanceTimersByTimeAsync(0);
		group.update();
		const calls = vi.mocked(collisions.clusterHorizontalCollisions).mock.calls
			.length;
		group.element.destroy({ children: true });
		await vi.advanceTimersByTimeAsync(DEFAULT_ROOT_COLLISION_THROTTLE);
		group.update();
		expect(collisions.clusterHorizontalCollisions).toHaveBeenCalledTimes(calls);
		expect(vi.getTimerCount()).toBe(0);
	});

	it("discards an in-flight cluster when its owner is destroyed", async () => {
		const pending = deferred<ReturnType<typeof leaf>>();
		createLeaf.mockImplementation((options: LeafOptions) =>
			options.type === "cluster"
				? pending.promise
				: Promise.resolve(leaf(options)),
		);
		const group = await factory({ isRoot: true });
		await group.render([datum("first"), datum("second")]);
		await vi.advanceTimersByTimeAsync(0);
		expect(createLeaf).toHaveBeenCalledWith({ type: "cluster" });
		group.element.destroy({ children: true });
		const cluster = leaf({ type: "cluster" });
		pending.resolve(cluster);
		await vi.advanceTimersByTimeAsync(DEFAULT_ROOT_COLLISION_THROTTLE);
		expect(cluster.element.destroyed).toBe(true);
		expect(cluster.render).not.toHaveBeenCalled();
	});

	it("destroys a late leaf without attaching or rendering it", async () => {
		const pending = deferred<ReturnType<typeof leaf>>();
		createLeaf.mockReturnValue(pending.promise);
		const group = await factory({ isRoot: true });
		const view = group.render([datum("first")]);
		group.element.destroy({ children: true });
		const item = leaf({ type: "event", event: datum("first") });
		pending.resolve(item);
		await view;
		expect(item.element.destroyed).toBe(true);
		expect(item.render).not.toHaveBeenCalled();
		expect(vi.getTimerCount()).toBe(0);
	});
});
