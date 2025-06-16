(function () {
  const form      = document.getElementById("configForm");
  const container = document.getElementById("extraColumnsContainer");
  const template  = document.getElementById("extraColumnTemplate");
  const addBtn    = document.getElementById("addExtraColumnBtn");

  // reset + remove any server‑side extras on load
  window.addEventListener("DOMContentLoaded", () => {
    form.reset();
    container.innerHTML = "";
  });

  function addExtraColumn() {
    const clone = template.cloneNode(true);
    clone.hidden = false;
    clone.removeAttribute("id");
    clone.querySelector(".removeColumnBtn")
         .addEventListener("click", () => container.removeChild(clone));
    container.appendChild(clone);
  }

  addBtn.addEventListener("click", addExtraColumn);
})();
