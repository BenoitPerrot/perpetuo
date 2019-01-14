import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '../perpetuo-list-editor/perpetuo-list-editor.js'

class PerpetuoDeploymentRequestProductSelector extends PolymerElement {

  static get template() {
    return html`
<perpetuo-list-editor id="main" label="Product Name"
                      choices="[[productNames]]" initial-count="10" selected-item="{{selectedProductName}}"
                      disabled="[[disabled]]"></perpetuo-list-editor>
`;
  }

  static get is() { return 'perpetuo-deployment-request-product-selector'; }

  static get properties() {
    return {
      disabled: Boolean,
      products: Array,
      productNames: { type: Array, computed: 'computeProductNames(products)' },
      selectedProductName: { type: String, notify: true },
    }
  }

  computeProductNames(products) {
    return products.filter(_ => _.active).map(_ => _.name).sort((a, b) => a.localeCompare(b));
  }

  clear() {
    if (this.$) {
      this.$.main.clear();
    }
  }
}

customElements.define(PerpetuoDeploymentRequestProductSelector.is, PerpetuoDeploymentRequestProductSelector);
