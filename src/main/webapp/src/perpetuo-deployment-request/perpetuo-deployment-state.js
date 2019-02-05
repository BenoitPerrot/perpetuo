import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

class PerpetuoDeploymentState extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  text-transform: uppercase;
  font-weight: 500;
  color: #555;
}
:host([state=deployed]) {
  color: #0f9d58;
}
:host([state$=Failed]),
:host([state$=Flopped]) {
  color: var(--perpetuo-red);
}
:host([state$=Progress]) {
  color: var(--perpetuo-blue);
}
</style>
<span>[[computeLabel(state)]]</span>
`;
  }

  static get is() { return 'perpetuo-deployment-state'; }

  static get properties() {
    return {
      state: { type: String, reflectToAttribute: true }
    }
  }

  computeLabel(state) {
    return state.replace(/([A-Z])/g, ' $1').toLowerCase();
  }
}

customElements.define(PerpetuoDeploymentState.is, PerpetuoDeploymentState);
