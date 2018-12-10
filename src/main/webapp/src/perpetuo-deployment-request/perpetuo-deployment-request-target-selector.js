import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-input/paper-input.js'

class PerpetuoDeploymentRequestTargetSelector extends PolymerElement {

  static get template() {
    return html`
<paper-input id="main" label="Target" disabled="[[disabled]]" on-value-changed="updateSelectedTargets"></paper-input>
`;
  }

  static get is() { return 'perpetuo-deployment-request-target-selector'; }

  static get properties() {
    return {
      disabled: Boolean,
      selectedTargets: { type: Object, value: null, notify: true },
    }
  }

  updateSelectedTargets() {
    this.selectedTargets = this.$.main.value;
  }

  clear() {
    if (this.$) {
      this.$.main.value = '';
    }
  }
}

customElements.define(PerpetuoDeploymentRequestTargetSelector.is, PerpetuoDeploymentRequestTargetSelector);
