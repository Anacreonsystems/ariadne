import { Application } from "pixi.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
	application,
	startApplication,
	stopApplication,
	waitForApplication,
} from "@/graphs/objects/application";
import { emitter } from "@/graphs/objects/events";

const { readStage } = vi.hoisted(() => ({
	readStage: vi.fn<() => Promise<HTMLDivElement>>(),
}));
vi.mock("@/graphs/objects/stage", () => ({ waitForStage: readStage }));

function deferred() {
	let resolve: () => void = () => {
		throw new Error("Uninitialized initialization wait");
	};
	const promise = new Promise<void>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

describe("owned asynchronous Pixi application initialization", () => {
	const created: Application[] = [];
	const initialized = new Set<Application>();
	let pending: ReturnType<typeof deferred> | undefined;

	beforeEach(() => {
		created.length = 0;
		initialized.clear();
		pending = undefined;
		readStage.mockResolvedValue(document.createElement("div"));
		vi.spyOn(Application.prototype, "init").mockImplementation(async function (
			this: Application,
		) {
			created.push(this);
			if (created.length === 1) await pending?.promise;
			initialized.add(this);
		});
		vi.spyOn(Application.prototype, "canvas", "get").mockImplementation(() =>
			document.createElement("canvas"),
		);
		vi.spyOn(Application.prototype, "destroy").mockImplementation(function (
			this: Application,
		) {
			expect(initialized.has(this)).toBe(true);
			this.stage.destroy({ children: true });
			Reflect.set(this, "stage", null);
		});
	});
	afterEach(() => {
		emitter.clear();
		stopApplication();
		vi.restoreAllMocks();
	});

	it("publishes only a fully initialized application", async () => {
		pending = deferred();
		const starting = startApplication();
		await vi.waitFor(() => expect(created).toHaveLength(1));
		expect(application).toBeNull();
		const ready = waitForApplication();
		pending.resolve();
		await starting;
		await expect(ready).resolves.toBe(created[0]);
	});

	it("defers destruction until an obsolete initialization has completed", async () => {
		const destroy = vi.spyOn(Application.prototype, "destroy");
		pending = deferred();
		const stage = document.createElement("div");
		readStage.mockResolvedValue(stage);
		const starting = startApplication();
		await vi.waitFor(() => expect(created).toHaveLength(1));
		const ownedStage = created[0].stage;
		stopApplication();
		expect(destroy).not.toHaveBeenCalled();
		pending.resolve();
		await starting;
		expect(destroy).toHaveBeenCalledOnce();
		expect(ownedStage.destroyed).toBe(true);
		expect(application).toBeNull();
		expect(stage.children).toHaveLength(0);
	});

	it("keeps a new scene when the previous scene finishes initialization later", async () => {
		pending = deferred();
		const oldStage = document.createElement("div");
		readStage.mockResolvedValueOnce(oldStage);
		const oldStart = startApplication();
		await vi.waitFor(() => expect(created).toHaveLength(1));
		const oldApplicationStage = created[0].stage;
		stopApplication();
		emitter.clear();
		const published = vi.fn();
		emitter.on("applicationCreated", published);
		const newStage = document.createElement("div");
		readStage.mockResolvedValue(newStage);
		await startApplication();
		expect(application).toBe(created[1]);
		pending.resolve();
		await oldStart;
		expect(application).toBe(created[1]);
		expect(oldApplicationStage.destroyed).toBe(true);
		expect(created[1].stage.destroyed).toBe(false);
		expect(oldStage.children).toHaveLength(0);
		expect(newStage.children).toHaveLength(1);
		expect(published).toHaveBeenCalledExactlyOnceWith(created[1]);
	});

	it("shares one pending initialization within the same live generation", async () => {
		pending = deferred();
		const first = startApplication();
		const second = startApplication();
		await vi.waitFor(() => expect(created).toHaveLength(1));
		pending.resolve();
		await Promise.all([first, second]);
		expect(created).toHaveLength(1);
		expect(application).toBe(created[0]);
	});

	it("does not clear the new pending initialization when the old one settles", async () => {
		const waits = [deferred(), deferred()];
		vi.spyOn(Application.prototype, "init").mockImplementation(async function (
			this: Application,
		) {
			created.push(this);
			await waits[created.length - 1].promise;
			initialized.add(this);
		});
		const oldStart = startApplication();
		await vi.waitFor(() => expect(created).toHaveLength(1));
		stopApplication();
		const nextStart = startApplication();
		await vi.waitFor(() => expect(created).toHaveLength(2));
		waits[0].resolve();
		await oldStart;
		const sharedStart = startApplication();
		await vi.waitFor(() => expect(created).toHaveLength(2));
		waits[1].resolve();
		await Promise.all([nextStart, sharedStart]);
		expect(created).toHaveLength(2);
		expect(application).toBe(created[1]);
	});
});
