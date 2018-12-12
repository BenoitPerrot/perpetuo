import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-card/paper-card.js'

import '../perpetuo-app/perpetuo-app-toolbar.js'
import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'
import '../perpetuo-product/perpetuo-product-table.js'

class PerpetuoProductsPage extends PolymerElement {

  static get template() {
    return html`
<style>
perpetuo-product-table {
  flex: 1;
}
</style>
<perpetuo-app-toolbar page-title="Perpetuo / Products"></perpetuo-app-toolbar>

<div style="display: flex; width: 100%;">
  <perpetuo-product-table id="productTable"></perpetuo-product-table>
</div>
`;
  }

  static get is() { return 'perpetuo-products-page'; }

  constructor() {
    super();

    this.client = new Perpetuo.Client();
  }

  connectedCallback() {
    super.connectedCallback();

    this.client.fetchProducts().then(products => {
      this.$.productTable.data = products;
    });
    this.client.getAllowedActions().then(actions => {
      this.$.productTable.canUpdateProduct = actions.includes("updateProduct") ? "enabled" : "disabled";
    });
  }
}

customElements.define(PerpetuoProductsPage.is, PerpetuoProductsPage);
