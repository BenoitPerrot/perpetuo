import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'

class PerpetuoIdentity extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: none;
}
</style>
`;
  }

  static get is() { return 'perpetuo-identity'; }

  static get properties() {
    return {
      login: { type: String, notify: true, readOnly: true }
    };
  }

  constructor() {
    super();

    this.client = new Perpetuo.Client();
  }

  connectedCallback() {
    super.connectedCallback();

    this.client.fetchIdentity().then(login => {
      this._setLogin(login);
    });
  }

  acknowledge() {
    const query = window.location.search.slice(1).split('&').reduce((m, p) => {
      const kv = p.split('=');
      return m.set(kv[0], kv[1]);
    }, new Map());

    this.client.identify(query.get('code')).then(() => {
      window.location.replace(decodeURIComponent(query.get('state')));
    });
  }

  request() {
    this.client.redirectToAuthorizeURL(window);
  }

  signOut() {
    this.client.signOut();
    this._setLogin(null);
  }
}

customElements.define(PerpetuoIdentity.is, PerpetuoIdentity);
