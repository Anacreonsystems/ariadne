import { scaleLinear } from "d3";
import { Container, Rectangle, Sprite, Texture } from "pixi.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flowRunArtifactFactory } from "@/graphs/factories/flowRunArtifact";
import { flowRunEventFactory } from "@/graphs/factories/flowRunEvent";
import { nodeFlowRunArtifactFactory } from "@/graphs/factories/nodeFlowRunArtifact";
import { nodeFlowRunEventFactory } from "@/graphs/factories/nodeFlowRunEvent";
import { emitter } from "@/graphs/objects/events";

const { primitive } = vi.hoisted(() => ({
	primitive: vi.fn<(kind: string) => Promise<ReturnType<typeof resource>>>(),
}));
vi.mock("@/graphs/objects", () => ({
	waitForApplication: () =>
		Promise.resolve({ screen: new Rectangle(0, 0, 500, 300) }),
	waitForViewport: () => Promise.resolve(new Container()),
}));
vi.mock("@/graphs/objects/scale", () => ({
	waitForScale: () => Promise.resolve(scaleLinear()),
}));
vi.mock("@/graphs/objects/settings", () => ({
	waitForSettings: () =>
		Promise.resolve({ disableEvents: false, disableArtifacts: false }),
	layout: { isTemporal: () => true },
}));
vi.mock("@/graphs/objects/styles", () => ({
	waitForStyles: () =>
		Promise.resolve({
			eventTargetSize: 20,
			eventBottomMargin: 4,
			eventSelectedBorderInset: 1,
			flowStateSelectedBarHeight: 5,
			artifactIconSize: 16,
			artifactPaddingLeft: 4,
			artifactPaddingRight: 4,
			artifactPaddingY: 2,
			artifactContentGap: 2,
		}),
}));
vi.mock("@/graphs/objects/flowRunEvents", () => ({
	waitForRunEvents: () => Promise.resolve([]),
}));
vi.mock("@/graphs/objects/selection", () => ({
	isSelected: () => false,
	selectItem: vi.fn(),
}));
vi.mock("@/graphs/factories/rectangle", () => ({
	rectangleFactory: () => primitive("rectangle"),
}));
vi.mock("@/graphs/factories/circle", () => ({
	circleFactory: () => primitive("circle"),
}));
vi.mock("@/graphs/factories/selectedBorder", () => ({
	selectedBorderFactory: () => primitive("border"),
}));
vi.mock("@/graphs/factories/label", () => ({
	nodeLabelFactory: () => primitive("label"),
}));
vi.mock("@/graphs/factories/icon", () => ({
	iconFactory: () => primitive("icon"),
}));
vi.mock("@/graphs/factories/circularProgressBar", () => ({
	circularProgressBarFactory: () => primitive("progress"),
}));
vi.mock("@/graphs/factories/artifactBar", () => ({
	artifactBarFactory: () => primitive("artifact-bar"),
}));

const date = new Date("2026-01-01T00:00:00Z");
const event = {
	id: "event-one",
	occurred: date,
	received: date,
	event: "prefect.task-run.Completed",
	payload: null,
	related: [],
	resource: { "prefect.resource.id": "task-one" },
};
const artifact = { id: "artifact-one", created: date, type: "result" as const };
const cluster = { ids: ["one", "two"], date };

const cases = [
	[
		"root event",
		async () => {
			const factory = await flowRunEventFactory({ type: "event", event });
			return { element: factory.element, render: () => factory.render() };
		},
	],
	[
		"nested event",
		async () => {
			const factory = await nodeFlowRunEventFactory({
				type: "event",
				event,
				parentStartDate: date,
			});
			return { element: factory.element, render: () => factory.render() };
		},
	],
	[
		"root event cluster",
		async () => {
			const factory = await flowRunEventFactory({ type: "cluster" });
			return {
				element: factory.element,
				render: () => factory.render(cluster),
			};
		},
	],
	[
		"nested event cluster",
		async () => {
			const factory = await nodeFlowRunEventFactory({
				type: "cluster",
				parentStartDate: date,
			});
			return {
				element: factory.element,
				render: () => factory.render(cluster),
			};
		},
	],
	[
		"root artifact",
		async () => {
			const factory = await flowRunArtifactFactory({
				type: "artifact",
				artifact,
			});
			return {
				element: factory.element,
				render: () => factory.render(artifact),
			};
		},
	],
	[
		"nested artifact",
		async () => {
			const factory = await nodeFlowRunArtifactFactory({
				type: "artifact",
				artifact,
				parentStartDate: date,
			});
			return {
				element: factory.element,
				render: () => factory.render(artifact),
			};
		},
	],
	[
		"root artifact cluster",
		async () => {
			const factory = await flowRunArtifactFactory({ type: "cluster" });
			return {
				element: factory.element,
				render: () => factory.render(cluster),
			};
		},
	],
	[
		"nested artifact cluster",
		async () => {
			const factory = await nodeFlowRunArtifactFactory({
				type: "cluster",
				parentStartDate: date,
			});
			return {
				element: factory.element,
				render: () => factory.render(cluster),
			};
		},
	],
] as const;

function resource(kind: string) {
	if (kind === "rectangle" || kind === "circle")
		return new Sprite(Texture.EMPTY);
	const element =
		kind === "border" || kind === "artifact-bar"
			? new Container()
			: new Sprite(Texture.EMPTY);
	return {
		element,
		render: vi.fn(() =>
			kind === "progress" ? element : Promise.resolve(element),
		),
	};
}

describe.each(cases)("%s owned subscriptions", (_name, create) => {
	beforeEach(() => {
		primitive.mockReset();
		primitive.mockImplementation((kind: string) =>
			Promise.resolve(resource(kind)),
		);
	});
	afterEach(() => {
		emitter.clear();
		vi.restoreAllMocks();
	});

	it("releases late subscriptions registered after the old scene was cleared", async () => {
		let resume: (() => void) | undefined;
		const pending = new Promise<void>((resolve) => {
			resume = resolve;
		});
		primitive.mockImplementationOnce(async (kind: string) => {
			await pending;
			return resource(kind);
		});
		const active = new Set<() => void>();
		const on = emitter.on;
		vi.spyOn(emitter, "on").mockImplementation((event, handler) => {
			const off = on(event, handler);
			const ownedOff = () => {
				active.delete(ownedOff);
				off();
			};
			active.add(ownedOff);
			return ownedOff;
		});
		const creating = create();
		await vi.waitFor(() => expect(primitive).toHaveBeenCalled());
		emitter.clear();
		active.clear();
		resume?.();
		const factory = await creating;
		expect(active.size).toBeGreaterThan(0);
		factory.element.destroy({ children: true });
		expect(active.size).toBe(0);
		expect(() => emitter.emit("scaleUpdated", scaleLinear())).not.toThrow();
		expect(() => emitter.emit("itemSelected", null)).not.toThrow();
	});

	it("does not position a destroyed leaf after an awaited render resumes", async () => {
		const factory = await create();
		const view = factory.render();
		factory.element.destroy({ children: true });
		await expect(view).resolves.toBeUndefined();
	});
});
