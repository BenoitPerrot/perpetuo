import {PolymerElement, html} from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/app-route/app-location.js'
import '/node_modules/@polymer/paper-styles/typography.js'
import '/node_modules/@polymer/iron-pages/iron-pages.js'

import '../perpetuo-app/perpetuo-app-drawer.js'
import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request-creation-page.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request-table.js'
import '../perpetuo-identity/perpetuo-identity.js'
import '../perpetuo-product/perpetuo-products-page.js'

class PerpetuoApp extends PolymerElement {

  static get template() {
    return html`
<style is="custom-style">
:host {
  flex: 1;
}
</style>

<perpetuo-identity id="identity" login="{{login}}"></perpetuo-identity>

<app-location route="{{route}}"></app-location>

<iron-pages id="pageSelector" selected="[[selectPage(route)]]" attr-for-selected="page-route" selected-attribute="active" on-drawer-toggle-tap="toggleDrawer">
  <perpetuo-deployment-request-table page-route="/deployment-requests"></perpetuo-deployment-request-table>
  <perpetuo-deployment-request-creation-page page-route="/deployment-requests/new" deployment-request-id="[[routeData.id]]"></perpetuo-deployment-request-creation-page>
  <perpetuo-deployment-request page-route="/deployment-requests/:id" deployment-request-id="[[routeData.id]]"></perpetuo-deployment-request>
  <perpetuo-products-page page-route="/products"></perpetuo-products-page>
</iron-pages>

<perpetuo-app-drawer id="drawer"></perpetuo-app-drawer>
`;
  }

  static get is() { return 'perpetuo-app'; }

  static get properties() {
    return {
      route: Object,
      routeData: { type: Object, value: () => ({}) },
      subroute: Object,
      subrouteData: Object
    };
  }

  constructor() {
    super();

    const versionRefreshPeriodInMs = 60 * 1000;
    const refreshVersion = _ => {
      fetch('/api/version').then(_ => _.text()).then(version => {
        if (window.localStorage.version !== version) {
          // TODO: Remove <<
          if (!window.localStorage.version || window.localStorage.version < 14838) {
            new Perpetuo.Client().signOut();
          }
          // >>
          window.localStorage.version = version;
          document.location.reload(true);
        }
      })
    };
    refreshVersion();
    setInterval(refreshVersion, versionRefreshPeriodInMs);
  }

  selectPage(route) {
    const routingTable = [
      {
        re: new RegExp('^/deployment-requests/(\\d+)'),
        f: (match) => {
          const id = match[1];
          this.routeData = { id: id };
          return '/deployment-requests/:id';
        }
      },
      {
        re: new RegExp('^/deployment-requests/new$'),
        f: (match) => {
          return '/deployment-requests/new';
        }
      },
      {
        re: new RegExp('^/deployment-requests/?$'),
        f: (match) => {
          return '/deployment-requests';
        }
      },
      {
        re: new RegExp('^/products/?$'),
        f: (match) => {
          return '/products';
        }
      },
      {
        re: new RegExp('^/identify'),
        f: (match) => {
          this.$.identity.acknowledge();
          return '';
        }
      },
    ];

    const page = routingTable.reduce((acc, routing) => {
      if (acc === null) {
        const m = route.path.match(routing.re);
        if (m) {
          return routing.f(m);
        }
      }
      return acc;
    }, null);
    // TODO: Have an error page for invalid routes, have a main page <<
    if (page === null) {
      return '/deployment-requests';
    }
    // >>
    return page;
  }

  toggleDrawer() {
    this.$.drawer.toggle();
  }
}

customElements.define(PerpetuoApp.is, PerpetuoApp);
