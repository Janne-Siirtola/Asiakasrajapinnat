// Script for updating exclude column count in the customer form
document.addEventListener('DOMContentLoaded', () => {
  const list     = document.getElementById('excludeList');
  const countEl  = document.getElementById('excludeCount');
  const checkboxes = list.querySelectorAll('input[name="exclude_columns"]');

  function updateCount() {
    const n = list.querySelectorAll('input[name="exclude_columns"]:checked').length;
    countEl.textContent = `${n} selected`;
  }
  window.updateExcludeCount = updateCount;

  // listen for every check/uncheck
  checkboxes.forEach(cb => cb.addEventListener('change', updateCount));

  // set initial value (in case some are pre‑checked)
  updateCount();
});

(function () {
  // Handles dynamic fields when creating a new customer
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

  // Hook up the click (in case the form doesn't include the button this will do nothing)
  if (addBtn) {
    addBtn.addEventListener('click', addExtraColumn);
  }
})();

(function () {
  // Client side behaviour for editing an existing customer
  const customersDataEl = document.getElementById("customersData");
  if (!customersDataEl) return; // not on the edit page

  const customers   = JSON.parse(customersDataEl.textContent);
  const listTitle   = document.getElementById("listTitle");
  const customerUl  = document.getElementById("customerList");
  const form        = document.getElementById("editForm");
  const formTitle   = document.getElementById("formTitle");
  const cancelBtn   = document.getElementById("cancelBtn");
  const deleteBtn   = document.getElementById("deleteBtn");
  const methodInput = document.getElementById("methodInput");

  /* helpers */
  const $ = id => form.querySelector("#" + id);
  const fields = {
    name: $("name"),
    konserni: $("konserni"),
    src_container: $("src_container"),
    dest_container: $("dest_container"),
    file_format: $("file_format"),
    file_encoding: $("file_encoding"),
    exclude_columns: $("exclude_columns"),
    extraContainer: document.getElementById("extraColumnsContainer"),
    enabled: $("enabled")
  };

  /* extra‑column template builder */
  const templateEl = document.getElementById('extraColumnTemplate');

  function makeExtraRow(key = "", info = { name: "", dtype: "" }) {
    // clone the <template> content
    const clone = templateEl.content.cloneNode(true);

    // get the root of this new group
    const group = clone.querySelector('.extra-columns-group');

    // wire up the remove button
    group.querySelector('.removeColumnBtn')
        .addEventListener('click', () => group.remove());

    // fill in the values
    group.querySelector('input[name="extra_key"]').value = key;
    group.querySelector('input[name="extra_name"]').value = info.name;
    group.querySelector('select[name="extra_dtype"]')
        .value = info.dtype || 'string';

    return group;
  }
  document.getElementById("addExtraColumnBtn")
          .addEventListener("click", () =>
            fields.extraContainer.appendChild(makeExtraRow())
          );

  /* list → form */
  customerUl.addEventListener("click", e => {
    const btn  = e.target.closest(".edit-btn");
    if (!btn) return;

    const cust = customers.find(c => c.name === btn.dataset.name);
    if (!cust) return console.error("Customer not found");

    formTitle.textContent = `Muokkaa: ${cust.name.toUpperCase()}`;
    fields.name.value            = cust.name;
    fields.konserni.value        = cust.konserni.join(",");
    fields.src_container.value   = (cust.source_container || "").replace("/", "");
    fields.dest_container.value  = (cust.destination_container || "").replace("/", "");
    fields.file_format.value     = cust.file_format || "";
    fields.file_encoding.value   = cust.file_encoding || "";
    fields.enabled.value        = cust.enabled.toString() || "";

    // ensure form submits as an edit unless changed later
    if (methodInput) methodInput.value = "edit_customer";

    fields.extraContainer.innerHTML = "";
    if (cust.extra_columns) {
      for (const [k, info] of Object.entries(cust.extra_columns)) {
        fields.extraContainer.appendChild(makeExtraRow(k, info));
      }
    }

    // find all of your exclude_columns checkboxes
    document
      .querySelectorAll('input[name="exclude_columns"]')
      .forEach(cb => {
        // check it if its value is in the preselected array
        cb.checked = cust.exclude_columns.includes(cb.value);
      });

    window.updateExcludeCount();

    customerUl.hidden = true;
    listTitle.hidden  = true;
    form.hidden       = false;
    form.scrollIntoView({ behavior:"smooth" });
  });

  /* cancel */
  cancelBtn.onclick = () => {
    form.hidden       = true;
    customerUl.hidden = false;
    listTitle.hidden  = false;
  };

  if (deleteBtn) {
    deleteBtn.addEventListener('click', () => {
      const name = fields.name.value;
      if (!name) return;
      if (confirm(`Poista asiakas '${name}'? Tätä toimintoa ei voi perua.` + 
        '\n\nTämä toiminto poistaa vain asiakkaan json-konfiguraatiotiedoston, mutta ei lähde- tai kohdekonttia. Poista lähde- ja kohdekontit erikseen, jos ne eivät ole enää tarpeellisia.')) {
        if (methodInput) methodInput.value = 'delete_customer';
        form.submit();
      }
    });
  }
})();
