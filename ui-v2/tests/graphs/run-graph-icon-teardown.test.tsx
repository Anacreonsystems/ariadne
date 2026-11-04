import { Texture } from "pixi.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { iconFactory } from "@/graphs/factories/icon";

const { getTexture } = vi.hoisted(() => ({
	getTexture: vi.fn<() => Promise<Texture>>(),
}));
vi.mock("@/graphs/textures/icon", () => ({ getIconTexture: getTexture }));
vi.mock("@/graphs/objects/culling", () => ({
	waitForIconCull: () => Promise.resolve({ add: vi.fn() }),
}));

describe("owned icon awaiting a shared texture", () => {
	afterEach(() => vi.restoreAllMocks());

	it("uses the texture while alive without taking ownership of the cache", async () => {
		const texture = new Texture({ source: Texture.EMPTY.source });
		const destroy = vi.spyOn(texture, "destroy");
		getTexture.mockResolvedValue(texture);
		const factory = await iconFactory({ cullAtZoomThreshold: false });
		await factory.render("ArtifactResult");
		expect(factory.element.texture).toBe(texture);
		factory.element.destroy();
		expect(destroy).not.toHaveBeenCalled();
	});

	it("does not attach a late shared texture to a destroyed sprite", async () => {
		const texture = new Texture({ source: Texture.EMPTY.source });
		const destroy = vi.spyOn(texture, "destroy");
		let resume: (texture: Texture) => void = () => {
			throw new Error("Uninitialized texture wait");
		};
		getTexture.mockReturnValue(
			new Promise<Texture>((resolve) => {
				resume = resolve;
			}),
		);
		const factory = await iconFactory({ cullAtZoomThreshold: false });
		const view = factory.render("ArtifactResult");
		factory.element.destroy();
		const attach = vi.spyOn(factory.element, "texture", "set");
		resume(texture);
		await view;
		expect(attach).not.toHaveBeenCalled();
		expect(destroy).not.toHaveBeenCalled();
	});
});
