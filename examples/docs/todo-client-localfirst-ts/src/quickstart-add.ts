const form = document.createElement("form");
const input = Object.assign(document.createElement("input"), {
  placeholder: "What needs to be done?",
});
form.append(input, Object.assign(document.createElement("button"), { textContent: "Add" }));
form.onsubmit = (e) => {
  e.preventDefault();
  db.insert(app.todos, { title: input.value, done: false });
  input.value = "";
};
document.body.append(form);
