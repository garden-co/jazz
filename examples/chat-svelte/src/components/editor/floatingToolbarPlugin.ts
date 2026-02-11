import type { EditorView } from 'prosemirror-view';
import { Plugin } from 'prosemirror-state';
import { getActiveMarks, getToolbarPosition, type MenuState, shouldShowToolbar } from './utils';

export function createFloatingToolbarPlugin(onUpdate: (state: MenuState) => void) {
  return new Plugin({
    view(view) {
      const updateMenu = (view: EditorView) => {
        const activeMarks = getActiveMarks(view);

        if (!shouldShowToolbar(view)) {
          onUpdate({ show: false, top: 0, left: 0, activeMarks });
          return;
        }

        const position = getToolbarPosition(view);
        onUpdate({
          show: true,
          ...position,
          activeMarks
        });
      };

      updateMenu(view);

      return {
        update(view) {
          updateMenu(view);
        }
      };
    }
  });
}
