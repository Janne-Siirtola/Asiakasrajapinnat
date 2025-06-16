(function () {
  // Grab the <template>, the container, and the “Add” button
  const template   = document.getElementById('extraColumnTemplate');
  const container  = document.getElementById('extraColumnsContainer');
  const addBtn     = document.getElementById('addExtraColumnBtn');

  function addExtraColumn() {
    // 1) Clone the template’s content (a DocumentFragment)
    const frag = template.content.cloneNode(true);
    // 2) Grab the new .extra-columns-group element
    const group = frag.querySelector('.extra-columns-group');
    // 3) Wire up its remove button
    group
      .querySelector('.removeColumnBtn')
      .addEventListener('click', () => group.remove());
    // 4) Append the cloned group into the container
    container.appendChild(frag);
  }

  // Hook up the click
  addBtn.addEventListener('click', addExtraColumn);
})();