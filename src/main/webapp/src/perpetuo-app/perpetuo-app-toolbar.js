import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/app-layout/app-toolbar/app-toolbar.js'
import '/node_modules/@polymer/iron-collapse/iron-collapse.js'
import '/node_modules/@polymer/iron-icon/iron-icon.js'
import '/node_modules/@polymer/iron-icons/iron-icons.js'
import '/node_modules/@polymer/iron-icons/social-icons.js'
import '/node_modules/@polymer/paper-button/paper-button.js'
import '/node_modules/@polymer/paper-icon-button/paper-icon-button.js'
import '/node_modules/@polymer/paper-styles/shadow.js'

import '../perpetuo-identity/perpetuo-identity.js'

function isChildOf(n, parent) {
  while (n) {
    if (n.parentElement === parent)
      return true;
    n = n.parentElement;
  }
  return false;
}

class PerpetuoAppToolbar extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  z-index: 1;
}
app-toolbar {
  background-color: var(--perpetuo-blue);
  color: #fff;
  font-size: 16pt;
}
paper-icon-button {
  --paper-icon-button-ink-color: white;
}
paper-icon-button + [main-title] {
  margin-left: 24px;
}
#title {
  margin-left: 24px;
  flex: 1;
}
#identityContainer {
  position: relative;
}
#identityContainer paper-button iron-icon {
  padding-right: 8px;
}
#identityContainer paper-button span {
  text-transform: initial;
}
#identityContainer #signOutContainer[hidden] {
  display: none;
}
#identityContainer #signOutContainer {
  position: absolute;
  width: 100%;
  background-color: #fff;
  color: #757575;
  @apply --shadow-elevation-2dp;
}
</style>
<perpetuo-identity id="identity" login="{{login}}"></perpetuo-identity>
<app-toolbar>
  <paper-icon-button icon="menu" on-tap="fireDrawerToggleTap"></paper-icon-button>
  <div id="title"><slot name="title"><span>[[pageTitle]]</span></slot></div>
  <div id="identityContainer">
    <paper-button style="min-width: 0;" on-tap="onIdentityTap"><iron-icon icon="social:person"></iron-icon><span>[[login]]</span></paper-button>
    <iron-collapse id="signOutContainer" hidden="[[!login]]">
      <paper-button on-tap="onSignOutTap"><iron-icon icon="exit-to-app"></iron-icon><span>Sign out</span></paper-button>
    </iron-collapse>
  </div>
</app-toolbar>
`;
  }

  static get is() { return 'perpetuo-app-toolbar'; }

  static get properties() {
    return {
      pageTitle: String,
      login: { type: String, notify: true }
    }
  }

  ready() {
    super.ready();
    const closeOnOutsideTap = tapEvent => {
      if (!isChildOf(tapEvent.detail.sourceEvent.target, this.$.identityContainer)) {
        this.$.signOutContainer.hide();
      }
    };
    this.$.signOutContainer.addEventListener('opened-changed', valueChangedEvent => {
      if (valueChangedEvent.detail.value) {
        document.addEventListener('tap', closeOnOutsideTap);
      } else {
        document.removeEventListener('tap', closeOnOutsideTap);
      }
    });
  }

  fireDrawerToggleTap() {
    this.dispatchEvent(new CustomEvent('drawer-toggle-tap', { bubbles: true, composed: true }));
  }

  onIdentityTap() {
    if (this.login)
      this.$.signOutContainer.toggle();
    else
      this.$.identity.request();
  }

  onSignOutTap() {
    this.$.identity.signOut();
    this.$.signOutContainer.opened = false;
  }
}

customElements.define(PerpetuoAppToolbar.is, PerpetuoAppToolbar);
