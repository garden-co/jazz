import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { beforeAll, describe, expect, it } from "vitest";
import { SchemaUnion } from "../coValues/schemaUnion.js";
import {
  Account,
  CoMap,
  CryptoProvider,
  co,
  loadCoValue,
  subscribeToCoValue,
} from "../exports.js";

class BaseWidget extends CoMap {
  type = co.string;
}

class RedButtonWidget extends BaseWidget {
  type = co.literal("button");
  color = co.literal("red");
  label = co.string;
}

class BlueButtonWidget extends BaseWidget {
  type = co.literal("button");
  color = co.literal("blue");
  label = co.string;
}

const ButtonWidget = SchemaUnion.Of<BaseWidget>((raw) => {
  switch (raw.get("color")) {
    case "red":
      return RedButtonWidget;
    case "blue":
      return BlueButtonWidget;
    default:
      throw new Error(`Unknown button color: ${raw.get("color")}`);
  }
});

class SliderWidget extends BaseWidget {
  type = co.literal("slider");
  min = co.number;
  max = co.number;
}

class CheckboxWidget extends BaseWidget {
  type = co.literal("checkbox");
  checked = co.boolean;
}

const getWidgetSchemaFromRaw = (raw: BaseWidget["_raw"]) => {
  switch (raw.get("type")) {
    case "button":
      return ButtonWidget;
    case "slider":
      return SliderWidget;
    case "checkbox":
      return CheckboxWidget;
    default:
      throw new Error(`Unknown widget type: ${raw.get("type")}`);
  }
};

class WidgetUnion extends SchemaUnion.Of<BaseWidget>(getWidgetSchemaFromRaw) {}

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

  it("should instantiate the correct subclass based on schema and provided data", async () => {
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

    const loadedButtonWidget = await loadCoValue(WidgetUnion, buttonWidget.id, {
      loadAs: me,
    });
    const loadedSliderWidget = await loadCoValue(WidgetUnion, sliderWidget.id, {
      loadAs: me,
    });
    const loadedCheckboxWidget = await loadCoValue(
      WidgetUnion,
      checkboxWidget.id,
      { loadAs: me },
    );

    expect(loadedButtonWidget).toBeInstanceOf(RedButtonWidget);
    expect(loadedSliderWidget).toBeInstanceOf(SliderWidget);
    expect(loadedCheckboxWidget).toBeInstanceOf(CheckboxWidget);
  });

  it("should integrate with subscribeToCoValue correctly", async () => {
    const buttonWidget = BlueButtonWidget.create(
      { type: "button", color: "blue", label: "Submit" },
      { owner: me },
    );
    let currentValue = "Submit";
    const unsubscribe = subscribeToCoValue(
      WidgetUnion,
      buttonWidget.id,
      { loadAs: me, syncResolution: true },
      (value: BaseWidget) => {
        if (value instanceof BlueButtonWidget) {
          expect(value.label).toBe(currentValue);
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
