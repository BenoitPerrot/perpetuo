import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-dropdown/iron-dropdown.js'
import '/node_modules/@polymer/iron-input/iron-input.js'
import '/node_modules/@polymer/paper-input/paper-input-container.js'
import '/node_modules/@polymer/paper-item/paper-item.js'
import '/node_modules/@polymer/paper-listbox/paper-listbox.js'
import '/node_modules/@polymer/paper-styles/shadow.js'

import '../perpetuo-chip/perpetuo-chip.js'
import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'
import { Suggester } from '../perpetuo-lib/perpetuo-suggester.js';

class PerpetuoListEditor extends PolymerElement {

  static get template() {
    return html`
<style>
paper-listbox {
  cursor: pointer;
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

:host(:not([selected-item])) perpetuo-chip {
  display: none;
}

#dropdown {
  @apply --shadow-elevation-2dp;
}
</style>
<paper-input-container always-float-label="[[selectedItem]]" disabled$="[[disabled]]">
  <label slot="label">[[label]]</label>
  <perpetuo-chip slot="prefix" label="[[selectedItem]]" disabled="[[disabled]]" index="[[index]]" on-delete-tap="removeElement"></perpetuo-chip>
  <iron-input slot="input"><input id="filterEditor" on-focus="onFilterEditorFocus" disabled$="[[computeInputDisabled(disabled, selectedItem)]]" on-keydown="onInputKeyDown" value="{{filter::input}}"></input></iron-input>
</paper-input-container>
<div>
  <iron-dropdown id="dropdown" no-auto-focus no-cancel-on-outside-click>
    <paper-listbox id="listbox" slot="dropdown-content" on-selected-changed="onSuggestionSelected">
      <template is="dom-repeat" items="[[filteredChoices]]">
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
      maxCount: { type: Number, value: Number.POSITIVE_INFINITY },
      label: String,
      filter: { type: String, notify: true },
      choices: Array,
      lruPath: String,
      filteredChoices: { type: Array, value: () => [], observer: 'onFilteredChoicesChanged' },
      selectedItem: { type: Object, notify: true, reflectToAttribute: true }
    };
  }

  static get observers() {
    return [
      'applyFilter(choices, maxCount, filter)'
    ];
  }

  ready() {
    super.ready();
    this.$.dropdown.focusTarget = this.$.filterEditor;

    this.addEventListener('blur', e => {
      this.$.dropdown.close();
    });
  }

  focus() {
    this.$.filterEditor.focus();
  }

  onFilterEditorFocus() {
    this.$.dropdown.open();
  }

  computeInputDisabled(disabled, _) {
    return disabled || this.selectedItem;
  }

  onInputKeyDown(e) {
    if (e.key === 'Backspace') {
      if (this.filter === '') {
        this.selectedItem = null;
      }
    } else if (e.key === 'ArrowDown') {
      this.$.listbox.focus();
    }
  }

  onFilteredChoicesChanged() {
    this.$.dropdown.notifyResize();
  }

  applyFilter(choices, maxCount, filter) {
    const preferred = this.lruPath ? Perpetuo.Util.readArrayFromLocalStorage(this.lruPath, maxCount) : choices;
    this.filteredChoices =
      (filter ? Perpetuo.Suggester.suggest(filter, choices, preferred) : preferred).slice(0, this.maxCount);
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
    this.selectedItem = value;
    this.$.listbox.select(null);
    this.$.dropdown.close();
    this.filter = '';
    if (this.lruPath) {
      Perpetuo.Util.writeArrayToLocalStorage(
        this.lruPath,
        new Perpetuo.LeastRecentlyUsedCache(this.maxCount, Perpetuo.Util.readArrayFromLocalStorage(this.lruPath, this.maxCount))
          .insert(value)
          .items
      );
      this.applyFilter(this.choices, this.maxCount, this.filter);
    }
  }

  removeElement(e) {
    this.selectedItem = null;
  }

  clear() {
    this.selectedItem = null;
    this.filter = '';
  }
}

customElements.define(PerpetuoListEditor.is, PerpetuoListEditor);
