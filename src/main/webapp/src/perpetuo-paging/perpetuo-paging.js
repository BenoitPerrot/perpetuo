import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-icons/iron-icons.js'
import '/node_modules/@polymer/paper-dropdown-menu/paper-dropdown-menu.js'
import '/node_modules/@polymer/paper-icon-button/paper-icon-button.js'
import '/node_modules/@polymer/paper-item/paper-item.js'
import '/node_modules/@polymer/paper-listbox/paper-listbox.js'

class PerpetuoPaging extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: flex;
  flex-direction: column;
}
.footer:not([hidden]) {
  display: flex;
  justify-content: flex-end;
  align-items: center;
}
paper-dropdown-menu {
  --paper-dropdown-menu-input: {
    text-align: right;
  }
  --paper-input-container-underline: {
    display: none;
  }
}
</style>
<slot></slot>
<div class="footer" hidden$="[[!hasEnoughItemToShowFooter(total, minItemCountForFooter)]]">
  <span>Items per page:</span>
  <paper-dropdown-menu style="width:4em;" no-label-float restore-focus-on-close no-animations vertical-align="bottom">
    <paper-listbox slot="dropdown-content" selected="{{pageSizeIndex}}">
      <template is="dom-repeat" items="[[pageSizes]]">
        <paper-item>[[item]]</paper-item>
      </template>
    </paper-listbox>
  </paper-dropdown-menu>
  <span style="margin-left:32px;">[[firstIndexOnPage]]-[[lastIndexOnPage]]</span>
  <span hidden$="[[!total]]" style="margin-left:10px;margin-right:32px;">of [[total]]</span>
  <paper-icon-button icon="chevron-left" on-tap="onPreviousPageTap" disabled="[[isFirstPage]]"></paper-icon-button>
  <paper-icon-button icon="chevron-right" on-tap="onNextPageTap" disabled="[[isLastPage]]"></paper-icon-button>
</div>
`;
  }

  static get is() { return 'perpetuo-paging'; }

  static get properties() {
    return {
      pageSizes: { type: Array },
      pageSizeIndex: { type: Number, value: 0 },
      pageSize: { type: Number, computed: 'computePageSize(pageSizes, pageSizeIndex)', notify: true },

      page: { type: Number, value: 1, notify: true },
      isFirstPage: { type: Boolean, computed: 'computeIsFirstPage(page)', notify: true },
      isLastPage: { type: Boolean, value: false },

      offset: { type: Number, computed: 'computeOffset(page, pageSize)', notify: true },
      total: { type: Number, value: null },

      firstIndexOnPage: { type: Number, computed: 'computeFirstIndexOnPage(offset)', notify: true },
      lastIndexOnPage: { type: Number, computed: 'computeLastIndexOnPage(offset, countOnPage)', notify: true },
      countOnPage: { type: Number },

      minItemCountForFooter: { type: Number, value: 0 }
    }
  }

  hasEnoughItemToShowFooter(total, minItemCountForFooter) {
    return (total === null || minItemCountForFooter < total);
  }

  selectPageSize(pageSize) {
    let i = this.pageSizes.findIndex(_ => _ === pageSize);
    if (i < 0) {
      this.push('pageSizes', pageSize);
      i = this.pageSizes.length - 1;
    }
    this.pageSizeIndex = i;
  }

  computePageSize(pageSizes, pageSizeIndex) {
    return pageSizes[pageSizeIndex];
  }

  computeOffset(page, pageSize) {
    return (page - 1) * pageSize;
  }

  computeFirstIndexOnPage(offset) {
    return 1 + offset;
  }

  computeLastIndexOnPage(offset, countOnPage) {
    return offset + countOnPage;
  }

  computeIsFirstPage(page) {
    return page === 1;
  }

  onPreviousPageTap() {
    if (!this.isFirstPage) {
         --this.page;
    }
  }

  onNextPageTap() {
    if (!this.isLastPage) {
        ++this.page;
    }
  }
}

customElements.define(PerpetuoPaging.is, PerpetuoPaging);
