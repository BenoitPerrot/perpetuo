import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-toggle-button/paper-toggle-button.js'

import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'

class PerpetuoProductTable extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: flex;
  flex-direction: column;
}

.table {
  flex: 1;
}
.row {
  display: flex;
  align-items: center;
  margin: 0.5em 40px;
  padding: 0.5em;
}
.row.heading {
  border-bottom: solid 1px #ddd;
  font-weight: 500;
  color: #757575;
}

span {
  display: inline-block;
}
span.product-type {
  flex: 0.5;
}
span.product-name {
  flex: 1;
}
span.active-status {
  flex: 1;
}
paper-toggle-button[class=disabled] {
  pointer-events: none;
}

</style>
<div class="row heading">
  <span class="product-name">Product Name</span>
  <span class="active-status">Active</span>
</div>
<div class="table">
  <template is="dom-repeat" items="[[data]]">
    <div class="row">
      <span class="product-name">[[item.name]]</span>
      <span class="active-status">
        <paper-toggle-button checked="{{item.active}}" on-tap="updateProduct" class$="[[canUpdateProduct]]"></paper-toggle-button>
      </span>
    </div>
  </template>
</div>
`;
  }

  static get is() { return 'perpetuo-product-table'; }

  static get properties() {
    return {
      data: { type: Array, value: () => [] },
      canUpdateProduct: String
    };
  }

  updateProduct(e) {
    this.client.updateProduct(e.model.item.name, e.model.item.active);
  }

  constructor() {
    super();

    this.client = new Perpetuo.Client();
  }
}

customElements.define(PerpetuoProductTable.is, PerpetuoProductTable);
