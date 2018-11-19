import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-dropdown/iron-dropdown.js'
import '/node_modules/@polymer/iron-input/iron-input.js'
import '/node_modules/@polymer/paper-input/paper-input-container.js'
import '/node_modules/@polymer/paper-item/paper-item.js'
import '/node_modules/@polymer/paper-listbox/paper-listbox.js'
import '/node_modules/@polymer/paper-styles/shadow.js'

import '../perpetuo-chip/perpetuo-chip.js'

class PerpetuoListEditor extends PolymerElement {

  static get template() {
    return html`
<style>
paper-listbox {
  cursor: pointer;
}

.chip-container {
  display: inline-flex;
}

paper-input-container input {
  height: 48px;
  @apply --paper-input-container-shared-input-style;
}

paper-input-container {
  --paper-input-container-label: {
    margin-top: 12px;
    margin-bottom: 10px;
  }
  --paper-input-container-label-floating: {
    margin-top: 0px;
  }
}

#dropdown {
  @apply --shadow-elevation-2dp;
}
</style>
<paper-input-container always-float-label="[[selectedItems.length]]" disabled$="[[disabled]]">
  <label slot="label">[[label]]</label>
  <div slot="prefix" class="chip-container">
    <template is="dom-repeat" items="[[selectedItems]]">
      <perpetuo-chip label="[[item]]" disabled="[[disabled]]" index="[[index]]" on-delete-tap="removeElement"></perpetuo-chip>
    </template>
  </div>
  <iron-input slot="input"><input id="filterEditor" on-focus="onFilterEditorFocus" disabled$="[[computeInputDisabled(disabled, multi, selectedItems.*)]]" on-keydown="onInputKeyDown" value="{{filter::input}}"></input></iron-input>
</paper-input-container>
<div>
  <iron-dropdown id="dropdown" no-auto-focus no-cancel-on-outside-click>
    <paper-listbox id="listbox" slot="dropdown-content" on-selected-changed="onSuggestionSelected">
      <template is="dom-repeat" items="[[filteredChoices]]" initial-count="[[initialCount]]">
        <paper-item>[[item]]</paper-item>
      </template>
    </paper-listbox>
  </iron-dropdown>
</div>
`;
  }

  static get is() { return 'perpetuo-list-editor'; }

  static get properties() {
    return {
      disabled: Boolean,
      initialCount: Number,
      label: String,
      multi: Boolean,
      filter: { type: String, notify: true },
      choices: Array,
      filteredChoices: { type: Array, computed: 'applyFilter(choices, filter)' },
      selectedItems: { type: Array, value: () => [], notify: true },
      selectedItem: { type: Object, notify: true, reflectToAttribute: true, computed: 'lastSelectedItem(selectedItems.*)' }
    };
  }

  ready() {
    super.ready();
    this.$.dropdown.focusTarget = this.$.filterEditor;

    this.addEventListener('blur', e => {
      this.$.dropdown.close();
    });
  }

  onFilterEditorFocus() {
    this.$.dropdown.open();
  }

  computeInputDisabled(disabled, multi, _) {
    return disabled || (!multi && 0 < this.selectedItems.length);
  }

  lastSelectedItem() {
    return this.selectedItems.length === 0 ? null : this.selectedItems[this.selectedItems.length - 1];
  }

  onInputKeyDown(e) {
    if (e.key === 'Backspace') {
      if (this.filter === '') {
        this.splice('selectedItems', -1, 1);
      }
    } else if (e.key === 'ArrowDown') {
      this.$.listbox.focus();
    }
  }

  applyFilter(choices, filter) {
    if (choices && choices.length) {
      if (filter && filter.length) {
        const words = filter.toLowerCase().split(' ');
        return this.choices.filter(c => words.every(w => c.toLowerCase().includes(w)));
      }
      else {
        return this.choices;
      }
    }
    return [];
  }

  onSuggestionSelected(e) {
    if (e.detail.value !== null) {
      this.addElement(this.filteredChoices[e.detail.value]);
    }
  }

  select(e) {
    const i = this.filteredChoices.findIndex(_ => _ == e);
    if (0 <= i) {
      this.$.listbox.selected = i;
    }
  }

  addElement(value) {
    if (this.multi)
      this.push('selectedItems', value);
    else
      this.selectedItems = [value];
    this.$.listbox.select(null);
    this.$.dropdown.close();
    this.filter = '';
  }

  removeElement(e) {
    this.splice('selectedItems', e.currentTarget.index, 1);
  }

  clear() {
    this.selectedItems = [];
    this.filter = '';
  }
}

customElements.define(PerpetuoListEditor.is, PerpetuoListEditor);
