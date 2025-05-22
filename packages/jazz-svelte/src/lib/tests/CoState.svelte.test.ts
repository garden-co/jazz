import { render, screen } from '@testing-library/svelte';
import { userEvent } from '@testing-library/user-event';
import { describe, expect, it, beforeEach } from 'vitest';
import { createJazzTestAccount, createJazzTestContext, setupJazzTestSync } from '../testing.js';
import UpdateNestedValue from './components/CoState/UpdateNestedValue.svelte';
import { Person } from './components/CoState/schema.js';
import { Dog } from './components/CoState/schema.js';
import type { Loaded } from 'jazz-tools';

beforeEach(async () => {
  await setupJazzTestSync();
  await createJazzTestAccount({
    isCurrentActiveAccount: true
  });
});

function setup(person: Loaded<typeof Person>) {
  const result = render(UpdateNestedValue, {
    context: createJazzTestContext(),
    props: { id: person.id }
  });

  return {
    ...result,
    nameInput: screen.getByLabelText('Name'),
    dogNameInput: screen.getByLabelText('Dog'),
    personNameOutput: screen.getByTestId('person-name'),
    personDogNameOutput: screen.getByTestId('person-dog-name')
  };
}
describe('CoState', () => {
  it('should show the and update properties', async () => {
    const person = Person.create(
      {
        name: 'John',
        age: 30,
        dog: Dog.create({
          name: 'Rex'
        })
      },
    );

    const { nameInput, personNameOutput } = setup(person);

    expect(nameInput).toHaveValue('John');

    await userEvent.clear(nameInput);
    await userEvent.type(nameInput, 'Jane');

    expect(nameInput).toHaveValue('Jane');
    expect(personNameOutput).toHaveTextContent('Jane');
  });

  it('should show the and update nested properties', async () => {
    const person = Person.create(
      {
        name: 'John',
        age: 30,
        dog: Dog.create({
          name: 'Rex'
        })
      },
    );

    const { dogNameInput, personDogNameOutput } = setup(person);

    expect(dogNameInput).toHaveValue('Rex');

    await userEvent.clear(dogNameInput);
    await userEvent.type(dogNameInput, 'Fuffy');

    expect(dogNameInput).toHaveValue('Fuffy');
    expect(personDogNameOutput).toHaveTextContent('Fuffy');
  });


  it('should react to id changes', async () => {
    const person = Person.create(
      {
        name: 'John',
        age: 30,
        dog: Dog.create({
          name: 'Rex'
        })
      },
    );

    const person2 = Person.create(
      {
        name: 'Jane',
        age: 30,
        dog: Dog.create({
          name: 'Fluffy'
        })
      },
    );

    const { personNameOutput, personDogNameOutput, rerender } = setup(person);

    await rerender({ id: person2.id });

    expect(personNameOutput).toHaveTextContent('Jane');
    expect(personDogNameOutput).toHaveTextContent('Fluffy');
  });
});
