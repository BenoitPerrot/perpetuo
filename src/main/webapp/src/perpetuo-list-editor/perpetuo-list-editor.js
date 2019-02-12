import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-input/iron-input.js'
import '/node_modules/@polymer/paper-input/paper-input-container.js'

import '../perpetuo-chip/perpetuo-chip.js'
import '../perpetuo-suggestion-dropdown/perpetuo-suggestion-dropdown.js'

class PerpetuoListEditor extends PolymerElement {

  static get template() {
    return html`
<style>
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
</style>
<paper-input-container always-float-label="[[selectedItem]]" disabled$="[[disabled]]">
  <label slot="label">[[label]]</label>
  <perpetuo-chip slot="prefix" label="[[selectedItem]]" disabled="[[disabled]]" index="[[index]]" on-delete-tap="removeElement"></perpetuo-chip>
  <iron-input slot="input"><input id="filterEditor" on-focus="onFilterEditorFocus" disabled$="[[computeInputDisabled(disabled, selectedItem)]]" on-keydown="onInputKeyDown" value="{{filter::input}}"></input></iron-input>
</paper-input-container>
<perpetuo-suggestion-dropdown id="dropdown" choices="[[choices]]" lru-path="[[lruPath]]" max-count="[[maxCount]]" filter="[[filter]]" on-suggestion-selected="onSuggestionSelected"></perpetuo-suggestion-dropdown>
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
      selectedItem: { type: Object, notify: true, reflectToAttribute: true }
    };
  }

  ready() {
    super.ready();

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
      this.$.dropdown.open();
      this.$.dropdown.focus();
    }
  }

  onSuggestionSelected(e) {
    if (e.detail.value !== null) {
      this.addElement(e.detail.value);
    }
  }

  select(e) {
    return this.$.dropdown.select(e);
  }

  addElement(value) {
    this.selectedItem = value;
    this.$.dropdown.select(null);
    this.$.dropdown.close();
    this.filter = '';
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
