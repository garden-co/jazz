import './app.css';
import 'jazz-tools/inspector/register-custom-element';
import { mount } from 'svelte';
import App from './App.svelte';

mount(App, {
  target: document.getElementById('root')!,
  props: {}
});
