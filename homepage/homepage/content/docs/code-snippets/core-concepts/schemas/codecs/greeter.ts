// [!code hide]
import { co, z } from "jazz-tools";
class Greeter {
  constructor(public name: string) {}

  greet() {
    console.log(`Hello, ${this.name}!`);
  }
}

const schema = co.map({
  greeter: z.codec(z.string(), z.z.instanceof(Greeter), {
    encode: (value) => value.name,
    decode: (value) => new Greeter(value),
  }),
});

const porter = schema.create({
  greeter: new Greeter("Alice"),
});

porter.greeter.greet();
