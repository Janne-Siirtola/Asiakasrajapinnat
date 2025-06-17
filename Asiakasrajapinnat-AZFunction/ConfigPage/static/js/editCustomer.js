(function () {
  // Client side behaviour for editing an existing customer
  const customers   = JSON.parse(
    document.getElementById("customersData").textContent
  );
  const listTitle   = document.getElementById("listTitle");
  const customerUl  = document.getElementById("customerList");
  const form        = document.getElementById("editForm");
  const formTitle   = document.getElementById("formTitle");
  const cancelBtn   = document.getElementById("cancelBtn");

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

    formTitle.textContent = `Edit Customer: ${cust.name.toUpperCase()}`;
    fields.name.value            = cust.name;
    fields.konserni.value        = cust.konserni.join(",");
    fields.src_container.value   = (cust.source_container || "").replace("/", "");
    fields.dest_container.value  = (cust.destination_container || "").replace("/", "");
    fields.file_format.value     = cust.file_format || "";
    fields.file_encoding.value   = cust.file_encoding || "";
    fields.enabled.value        = cust.enabled.toString() || "";

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
})();


