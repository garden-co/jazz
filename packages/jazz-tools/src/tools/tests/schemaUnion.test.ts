import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  Account,
  CryptoProvider,
  Loaded,
  co,
  coValueClassFromCoValueClassOrSchema,
  subscribeToCoValue,
  z,
} from "../exports.js";
import { createJazzTestAccount } from "../testing.js";
import { assertLoaded } from "./utils.js";

const RedButtonWidget = co.map({
  type: z.literal("button"),
  color: z.literal("red"),
  label: z.string(),
  disabled: z.boolean().optional(),
});

const BlueButtonWidget = co.map({
  type: z.literal("button"),
  color: z.literal("blue"),
  label: z.string(),
  blueness: z.number(),
  disabled: z.boolean().optional(),
});

const ButtonWidget = co.discriminatedUnion("color", [
  RedButtonWidget,
  BlueButtonWidget,
]);

const SliderWidget = co.map({
  type: z.literal("slider"),
  min: z.number(),
  max: z.number(),
});

const CheckboxWidget = co.map({
  type: z.literal("checkbox"),
  checked: z.boolean(),
});

const WidgetUnion = co.discriminatedUnion("type", [
  ButtonWidget,
  SliderWidget,
  CheckboxWidget,
]);

describe("SchemaUnion", () => {
  let Crypto: CryptoProvider;
  let me: Account;

  beforeAll(async () => {
    Crypto = await WasmCrypto.create();
    me = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });
  });

  it("should instantiate the correct schema based on schema and provided data", async () => {
    const buttonWidget = RedButtonWidget.create(
      { type: "button", color: "red", label: "Submit" },
      { owner: me },
    );
    const sliderWidget = SliderWidget.create(
      { type: "slider", min: 0, max: 100 },
      { owner: me },
    );
    const checkboxWidget = CheckboxWidget.create(
      { type: "checkbox", checked: true },
      { owner: me },
    );

    const loadedButtonWidget = await WidgetUnion.load(buttonWidget.$jazz.id, {
      loadAs: me,
    });
    const loadedSliderWidget = await WidgetUnion.load(sliderWidget.$jazz.id, {
      loadAs: me,
    });
    const loadedCheckboxWidget = await WidgetUnion.load(
      checkboxWidget.$jazz.id,
      {
        loadAs: me,
      },
    );

    assertLoaded(loadedButtonWidget);
    expect(loadedButtonWidget.type).toBe("button");
    assertLoaded(loadedSliderWidget);
    expect(loadedSliderWidget.type).toBe("slider");
    assertLoaded(loadedCheckboxWidget);
    expect(loadedCheckboxWidget.type).toBe("checkbox");
  });

  it("should integrate with subscribeToCoValue correctly", async () => {
    const buttonWidget = BlueButtonWidget.create(
      { type: "button", color: "blue", label: "Submit", blueness: 100 },
      { owner: me },
    );
    let currentValue = "Submit";
    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(WidgetUnion),
      buttonWidget.$jazz.id,
      { loadAs: me, syncResolution: true },
      (value: Loaded<typeof WidgetUnion>) => {
        if (value.type === "button") {
          expect(value.label).toBe(currentValue);
          assert(value.color === "blue");
          expect(value.blueness).toBe(100);
        } else {
          throw new Error("Unexpected widget type");
        }
      },
    );
    currentValue = "Changed";
    buttonWidget.$jazz.set("label", "Changed");
    unsubscribe();
  });

  describe("methods available on all members of the union type are available on the union", () => {
    const Buttons = co.list(ButtonWidget);
    let buttons: ReturnType<typeof Buttons.create>;

    beforeEach(() => {
      buttons = Buttons.create([
        {
          type: "button",
          color: "red",
          label: "Delete",
          disabled: true,
        },
      ]);
    });

    it("$jazz.applyDiff", () => {
      assert(buttons[0]);
      buttons[0].$jazz.applyDiff({ label: "Remove" });
      expect(buttons[0].label).toBe("Remove");
    });

    it("$jazz.delete", () => {
      assert(buttons[0]);

      buttons[0].$jazz.delete("disabled");

      expect(buttons[0].disabled).toBeUndefined();
    });
  });

  describe("methods with constrained type parameters are not available on the union", () => {
    it("$jazz.set", () => {
      const Buttons = co.list(ButtonWidget);
      const buttons = Buttons.create([
        {
          type: "button",
          color: "red",
          label: "Delete",
        },
      ]);

      assert(buttons[0]);
      // @ts-expect-error Typescript does not allow calling the union of methods with constrained type parameters
      buttons[0].$jazz.set("label", "Remove");
      expect(buttons[0].label).toBe("Remove");
    });
  });
});
