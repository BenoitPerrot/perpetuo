import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '../perpetuo-deployment-request/perpetuo-deployment-request-target-selector.js'

class PerpetuoDeploymentRequestStrategySelector extends PolymerElement {

  static get template() {
    return html`
<perpetuo-deployment-request-target-selector product-name="[[productName]]" id="targetSelector"
                                             on-selected-targets-changed="updateSelectedStrategy">
</perpetuo-deployment-request-target-selector>
`;
  }

  static get is() { return 'perpetuo-deployment-request-strategy-selector'; }

  static get properties() {
    return {
      productName: String,

      selectedStrategy: { type: Array, value: () => [], notify: true },
    }
  }

  clear() {
    if (this.$) {
      this.$.targetSelector.clear();
    }
  }

  updateSelectedStrategy() {
    this.selectedStrategy =
    0 < this.$.targetSelector.selectedTargets.length ? [{ name: '', target: this.$.targetSelector.selectedTargets }] : [];
  }
}

customElements.define(PerpetuoDeploymentRequestStrategySelector.is, PerpetuoDeploymentRequestStrategySelector);
