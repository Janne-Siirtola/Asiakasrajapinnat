(function () {
  const template  = document.getElementById('baseColumnTemplate');
  const container = document.getElementById('baseColumnsContainer');
  const addBtn    = document.getElementById('addBaseColumnBtn');

  let dragSrcEl = null;

  /* ── DnD handlers (same as before) ── */
  function handleDragStart(e) {
    dragSrcEl = this;
    e.dataTransfer.effectAllowed = 'move';
    this.classList.add('dragging');
  }
  function handleDragOver(e) {
    e.preventDefault();
    return false;
  }
  function handleDrop(e) {
    e.stopPropagation();
    if (dragSrcEl !== this) {
      const nodes     = Array.from(container.children);
      const fromIndex = nodes.indexOf(dragSrcEl);
      const toIndex   = nodes.indexOf(this);
      if (fromIndex < toIndex) {
        container.insertBefore(dragSrcEl, this.nextSibling);
      } else {
        container.insertBefore(dragSrcEl, this);
      }
    }
    return false;
  }
  function handleDragEnd(e) {
    this.classList.remove('dragging');
  }
  function handleDragEnter() { this.classList.add('over'); }
  function handleDragLeave() { this.classList.remove('over'); }

  /* attach DnD listeners to a row */
  function addDnDHandlers(group) {
    // group is draggable only when handle tells it so
    group.draggable = false;
    group.addEventListener('dragstart', handleDragStart);
    group.addEventListener('dragenter', handleDragEnter);
    group.addEventListener('dragover', handleDragOver);
    group.addEventListener('dragleave', handleDragLeave);
    group.addEventListener('drop', handleDrop);
    group.addEventListener('dragend', e => {
      handleDragEnd.call(group, e);
      group.draggable = false;  // reset after drag
    });
  }

  /* setup one row: remove/move toggles + decimals toggle + DnD + drag-handle wiring */
  function attachHandlers(group) {
    // remove button
    group.querySelector('.removeColumnBtn')
         .addEventListener('click', () => group.remove());

    // show/hide decimals
    const dtypeSelect   = group.querySelector('select[name="dtype"]');
    const decimalsGroup = group.querySelector('.decimals-group');
    function toggleDecimals() {
      decimalsGroup.style.display =
        dtypeSelect.value === 'float' ? '' : 'none';
    }
    dtypeSelect.addEventListener('change', toggleDecimals);
    toggleDecimals();

    // DnD setup
    addDnDHandlers(group);

    // hook the handle so only it can activate dragging
    const handle = group.querySelector('.drag-handle');
    handle.addEventListener('mousedown', () => {
      group.draggable = true;
    });
  }

  /* clone & append a new row */
  function addRow() {
    const frag  = template.content.cloneNode(true);
    const group = frag.querySelector('.base-columns-group');
    attachHandlers(group);
    container.appendChild(group);
  }

  // initialize existing rows
  document.querySelectorAll('#baseColumnsContainer .base-columns-group')
          .forEach(attachHandlers);
  addBtn.addEventListener('click', addRow);
})();
