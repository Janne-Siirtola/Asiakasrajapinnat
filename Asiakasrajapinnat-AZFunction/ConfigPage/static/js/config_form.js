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
