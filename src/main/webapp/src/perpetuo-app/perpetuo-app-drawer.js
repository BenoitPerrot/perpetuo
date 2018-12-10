import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/app-layout/app-drawer/app-drawer.js'
import '/node_modules/@polymer/iron-icon/iron-icon.js'
import '/node_modules/@polymer/iron-icons/iron-icons.js'
import '/node_modules/@polymer/paper-item/paper-icon-item.js'

class PerpetuoAppDrawer extends PolymerElement {

  static get template() {
    return html`
<style>
app-drawer {
  --app-drawer-scrim-background: rgba(255, 255, 255, 0.5);
  --app-drawer-content-container: {
    background-color: #333;
  }
  z-index: 1;
}
app-drawer a {
  text-decoration: none;
  color: #eee;
}
</style>
<app-drawer id="drawer" swipe-open>
  <a href="/" on-tap="close"><paper-icon-item><iron-icon icon="menu" slot="item-icon"></iron-icon>Perpetuo</paper-icon-item></a>
  <a href="/deployment-requests" on-tap="close"><paper-icon-item><iron-icon icon="unarchive" slot="item-icon"></iron-icon>Deployment Requests</paper-icon-item></a>
  <a href="/products" on-tap="close"><paper-icon-item><iron-icon icon="folder-open" slot="item-icon"></iron-icon>Products</paper-icon-item></a>
</app-drawer>
`;
  }

  static get is() { return 'perpetuo-app-drawer'; }

  toggle() {
    this.$.drawer.toggle();
  }

  close() {
    this.$.drawer.close();
  }
}

customElements.define(PerpetuoAppDrawer.is, PerpetuoAppDrawer);
