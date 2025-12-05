import { AppRegistry } from 'react-native';
// @ts-expect-error Illustrative only, not a real import here
import App from './App';
import { name as appName } from './app.json';
// [!code ++:1]
import 'jazz-tools/react-native/polyfills';

AppRegistry.registerComponent(appName, () => App);