import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-dropdown-menu/paper-dropdown-menu.js'
import '/node_modules/@polymer/paper-fab/paper-fab.js'
import '/node_modules/@polymer/paper-item/paper-item.js'
import '/node_modules/@polymer/paper-listbox/paper-listbox.js'

import '../perpetuo-app/perpetuo-app-toolbar.js'
import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request-entry.js'
import '../perpetuo-identity/perpetuo-identity.js'
import '../perpetuo-list-editor/perpetuo-list-editor.js'
import '../perpetuo-paging/perpetuo-paging.js'

class WindowLocationHelper {
  static getQueryParams(win) {
    const s = win.location.search.slice(1);
    return new Map(s.length === 0 ? [] : s.split('&').map(_ => _.split('=')));
  }

  static setQueryParam(win, k, v) {
    const queryParams = WindowLocationHelper.getQueryParams(win);
    if (v === undefined) {
      queryParams.delete(k);
    } else {
      queryParams.set(k, v);
    }
    const search = Array.from(queryParams.entries()).map(_ => _.join('=')).join('&');
    const query = search ? `?${search}` : ''
    const url = `${win.location.protocol}//${win.location.host}${win.location.pathname}${query}`;
    win.history.pushState({}, '', url);
  }
}

class PerpetuoDeploymentRequestTable extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: flex;
  flex-direction: column;
}

.table {
  flex: 1;
  padding: 1em 40px;
  margin-left: 50px;
}
.row.heading {
  flex: 1;
  display: flex;
  align-items: center;
  padding: 0.5em 0;
  border-bottom: solid 1px #ddd;
  color: #757575;
}
.row.heading span {
  font-weight: 500;
}
.product-name {
  width: 40%;
}
.version {
  width: 10%;
}
.target {
  width: 20%;
}
.creation-date {
  width: 20%;
}
div.state {
  width: 10%;
  display: flex;
  justify-content: flex-end;
}
</style>
<perpetuo-identity id="identity" login="{{login}}"></perpetuo-identity>
<perpetuo-app-toolbar page-title="Perpetuo / Deployment Requests"></perpetuo-app-toolbar>

<div sticky class="table" style="background: #fff">
  <div class="row heading">
    <span class="product-name">Product</span>
    <span class="version">Version</span>
    <span class="target">Target</span>
    <span class="creation-date">Creation Date</span>
    <div class="state">
      <span>State</span>
    </div>
  </div>
  <div style="display: flex;">
    <div class="product-name">
      <perpetuo-list-editor id="productFilter" key="productName" label="Filter" choices="[[productNames]]" lru-path="productSelector.lru" max-count="10" on-selected-item-changed="updateFilter"></perpetuo-list-editor>
    </div>
    <div class="version"></div>
    <span class="target"></span>
    <div class="creation-date">
      <paper-dropdown-menu label="Time Zone" no-animations>
        <paper-listbox slot="dropdown-content" selected="0" selected-item="{{timeZoneItem}}">
          <paper-item timestamp-converter="timestampToUTCDate">UTC +0</paper-item>
          <paper-item timestamp-converter="timestampToLocalDate">Local</paper-item>
        </paper-listbox>
      </paper-dropdown-menu>
    </div>
    <div class="state"></div>
  </div>
</div>
<a href="/deployment-requests/new"><paper-fab icon="add" style="position:fixed; left: 20px; top: 105px; background: #ff8f1c;"></paper-fab></a>
<perpetuo-paging id="paging"
                 page-sizes="[20,50,100]" page-size="{{pageSize}}"
                 page="{{page}}" is-last-page="[[!hasNextPage]]"
                 count-on-page="[[data.length]]"
                 class="table">
  <template is="dom-repeat" items="[[data]]">
    <perpetuo-deployment-request-entry data="[[item]]"></perpetuo-deployment-request-entry>
  </template>
</perpetuo-paging>
`;
  }

  static get is() { return 'perpetuo-deployment-request-table'; }

  static get properties() {
    return {
      login: String,
      queryParams: Object,
      active: { type: Boolean, observer: 'onActiveChanged' },
      data: { type: Array, observer: 'convertTimestamp' },
      indices: { type: Object, value: () => new Object() },

      productNames: { type: Array, value: () => [] },
      timeZoneItem: Object,
      timestampConverter: { type: String, computed: 'computeTimestampConverter(timeZoneItem)', observer: 'convertTimestamp' },

      page: { type: Number, value: 1, observer: 'onPageChanged', notify: true },
      hasNextPage: Boolean,
      pageSize: { type: Number, observer: 'onPageSizeChanged' }
    };
  }

  computeTimestampConverter(timeZoneItem) {
    return timeZoneItem ? timeZoneItem.getAttribute('timestamp-converter') : null;
  }

  constructor() {
    super();

    this.client = new Perpetuo.Client();
    this.refresher = new Perpetuo.AsyncRepeater(() => this.refresh(), 1000);
  }

  ready() {
    super.ready();

    window.addEventListener('popstate', _ => {
      this.applyQueryParams();
    });
  }

  applyQueryParams() {
    const queryParams = WindowLocationHelper.getQueryParams(window);

    this.deploymentRequestFilter = decodeURIComponent(queryParams.get('q') || '');
    if (this.deploymentRequestFilter) {
      this.$.productFilter.select(this.deploymentRequestFilter);
    } else {
      this.$.productFilter.clear();
    }
    const page = parseInt(queryParams.get('page'));
    if (0 < page)
      this.page = page;
    const pageSize = parseInt(queryParams.get('page-size'));
    if (0 < pageSize)
      this.$.paging.selectPageSize(pageSize);
  }

  onActiveChanged(active) {
    if (active) {
      this.applyQueryParams();
      this.client.fetchProducts().then(products => {
        this.productNames = products.sort((a, b) => {
          if (a.active === b.active) { return a.name.localeCompare(b.name); }
          if (a.active) { return -1; }
          return 1;
        }).map(_ => _.name);
        this.$.productFilter.select(this.deploymentRequestFilter);
      });
    }
    this.refresher.suspended = !active;
  }

  refresh(forced) {
    const query = Object.assign(
      {
        offset: this.$.paging.offset,
        limit: this.pageSize
      },
      this.deploymentRequestFilter ? { where: [{ 'field': 'productName', 'equals': this.deploymentRequestFilter }] } : {}
    );
    return this.client.fetchDeploymentRequests(query).then(deploymentRequests => {
      this.data = deploymentRequests;
      this.hasNextPage = this.client.hasMoreDeploymentRequests;
      if (forced) {
        this.page = 1;
      }
    });
  }

  convertTimestamp() {
    if (this.data && this.timestampConverter) {
      const f = this.get(this.timestampConverter);
      this.data.forEach(obj => obj.dateString = f(obj.creationDate));
    }
  }

  timestampToUTCDate(unixTimestamp) {
    return new Date(unixTimestamp * 1000).toISOString().replace('T', ' ').replace(/-0/g, '-').split('.')[0];
  }

  timestampToLocalDate(unixTimestamp) {
    const date = new Date(unixTimestamp * 1000);
    const time = date.toTimeString().split(' ')[0];
    return date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate() + ' ' + time;
  }

  updateFilter(e) {
    if (this.active) {
      if (this.deploymentRequestFilter != e.target.selectedItem) {
        this.deploymentRequestFilter = e.target.selectedItem;
        const q = this.deploymentRequestFilter ? encodeURIComponent(this.deploymentRequestFilter) : undefined;
        WindowLocationHelper.setQueryParam(window, 'q', q);
        this.refresh(true);
      }
    }
  }

  onPageChanged() {
    if (this.active) {
      this.hasNextPage = false; // Assume no more page
      WindowLocationHelper.setQueryParam(window, 'page', this.page);
      this.refresh();
    }
  }

  onPageSizeChanged() {
    if (this.active) {
      WindowLocationHelper.setQueryParam('page-size', this.pageSize);
      this.refresh();
    }
  }
}

customElements.define(PerpetuoDeploymentRequestTable.is, PerpetuoDeploymentRequestTable);
