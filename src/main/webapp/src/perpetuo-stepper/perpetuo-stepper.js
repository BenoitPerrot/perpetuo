import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

class PerpetuoStepper extends PolymerElement {

  static get template() {
    return html`
<slot></slot>
`;
  }

  static get is() { return 'perpetuo-stepper'; }

  ready() {
    super.ready();

    this.stepNodes = this.shadowRoot.querySelector('slot').assignedNodes().filter(_ => _.nodeType === 1);
    this.stepNodes.forEach((stepNode, i) => {
      stepNode.index = i + 1;
      stepNode.addEventListener('step-enter', e => {
        stepNode.active = true;
      });
      stepNode.addEventListener('step-leave', e => {
        stepNode.active = false;
      });
      stepNode.addEventListener('step-completed', e => {
        stepNode.dispatchEvent(new CustomEvent('step-leave'));
        if (i + 1 < this.stepNodes.length) {
          const nextStepNode = this.stepNodes[i + 1];
          nextStepNode.dispatchEvent(new CustomEvent('step-enter'));
        } else {
          this.dispatchEvent(new CustomEvent('stepper-completed'));
        }
      });
      stepNode.addEventListener('step-cancelled', e => {
        stepNode.dispatchEvent(new CustomEvent('step-leave'));
        if (0 < i) {
          const prevStepNode = this.stepNodes[i - 1];
          prevStepNode.dispatchEvent(new CustomEvent('step-enter'));
        } else {
          this.dispatchEvent(new CustomEvent('stepper-cancelled'));
        }
      })
    });
    this.reset();
  }

  reset() {
    if (this.stepNodes && 0 < this.stepNodes.length) {
      this.stepNodes.forEach(stepNode => {
        stepNode.active = false;
        stepNode.completed = false;
      });
      this.stepNodes[0].dispatchEvent(new CustomEvent('step-enter'));
    }
  }
}

customElements.define(PerpetuoStepper.is, PerpetuoStepper);
