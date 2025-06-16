(function () {
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
    exclude_columns: $("exclude_columns"),
    extraContainer: document.getElementById("extraColumnsContainer")
  };

  /* extra‑column template builder */
  function makeExtraRow(key = "", info = { name:"", dtype:"" }) {
    const div = document.createElement("div");
    div.className = "extra-columns-group";
    div.innerHTML = `
      <button type="button" class="removeColumnBtn">Poista</button>
      <div class="form-group"><label>Avaimen nimi</label>
        <input type="text" name="extra_key" value="${key}">
      </div>
      <div class="form-group"><label>Näyttönimi</label>
        <input type="text" name="extra_name" value="${info.name}">
      </div>
      <div class="form-group"><label>Tietotyyppi</label>
        <select name="extra_dtype">
          <option value="string">String</option>
          <option value="float">Float</option>
        </select>
      </div>`;
    div.querySelector(".removeColumnBtn").onclick = () => div.remove();
    div.querySelector("select[name='extra_dtype']").value = info.dtype || "string";
    return div;
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
    fields.file_format.value     = cust.file_format;
    fields.exclude_columns.value = (cust.exclude_columns || []).join(",");

    fields.extraContainer.innerHTML = "";
    if (cust.extra_columns) {
      for (const [k, info] of Object.entries(cust.extra_columns)) {
        fields.extraContainer.appendChild(makeExtraRow(k, info));
      }
    }

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
