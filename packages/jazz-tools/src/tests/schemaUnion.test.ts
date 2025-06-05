import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, beforeAll, describe, expect, it } from "vitest";
import {
  Account,
  CryptoProvider,
  Loaded,
  anySchemaToCoSchema,
  co,
  loadCoValue,
  subscribeToCoValue,
  z,
} from "../exports.js";

const RedButtonWidget = co.map({
  type: z.literal("button"),
  color: z.literal("red"),
  label: z.string(),
});

const BlueButtonWidget = co.map({
  type: z.literal("button"),
  color: z.literal("blue"),
  label: z.string(),
  blueness: z.number(),
});

const ButtonWidget = z.discriminatedUnion("type", [
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

const WidgetUnion = z.discriminatedUnion("type", [
  ButtonWidget,
  SliderWidget,
  CheckboxWidget,
]);

describe("SchemaUnion", () => {
  let Crypto: CryptoProvider;
  let me: Account;

  beforeAll(async () => {
    Crypto = await WasmCrypto.create();
    me = await Account.create({
      creationProps: { name: "Hermes Puggington" },
      crypto: Crypto,
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

    const loadedButtonWidget = await loadCoValue(
      anySchemaToCoSchema(WidgetUnion),
      buttonWidget.id,
      {
        loadAs: me,
      },
    );
    const loadedSliderWidget = await loadCoValue(
      anySchemaToCoSchema(WidgetUnion),
      sliderWidget.id,
      {
        loadAs: me,
      },
    );
    const loadedCheckboxWidget = await loadCoValue(
      anySchemaToCoSchema(WidgetUnion),
      checkboxWidget.id,
      { loadAs: me },
    );

    expect(loadedButtonWidget?.type).toBe("button");
    expect(loadedSliderWidget?.type).toBe("slider");
    expect(loadedCheckboxWidget?.type).toBe("checkbox");
  });

  it("should integrate with subscribeToCoValue correctly", async () => {
    const buttonWidget = BlueButtonWidget.create(
      { type: "button", color: "blue", label: "Submit", blueness: 100 },
      { owner: me },
    );
    let currentValue = "Submit";
    const unsubscribe = subscribeToCoValue(
      anySchemaToCoSchema(WidgetUnion),
      buttonWidget.id,
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
    buttonWidget.label = "Changed";
    unsubscribe();
  });
});
