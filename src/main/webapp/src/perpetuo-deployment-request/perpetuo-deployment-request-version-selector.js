import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-input/paper-input.js'

class PerpetuoDeploymentRequestVersionSelector extends PolymerElement {

  static get template() {
    return html`
<paper-input id="main" label="Version" disabled="[[disabled]]" on-value-changed="updateSelectedVersion"></paper-input>
`;
  }

  static get is() { return 'perpetuo-deployment-request-version-selector'; }

  static get properties() {
    return {
      disabled: Boolean,
      selectedVersion: { type: String, notify: true },
    }
  }

  updateSelectedVersion() {
    this.selectedVersion = this.$.main.value.trim();
  }

  clear() {
    if (this.$) {
      this.$.main.value = '';
    }
  }
}

customElements.define(PerpetuoDeploymentRequestVersionSelector.is, PerpetuoDeploymentRequestVersionSelector);
