(function () {
  const form = document.getElementById('manualRunForm');
  const statusContainer = document.getElementById('statusContainer');
  if (!form || !statusContainer) return;

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn = form.querySelector('button[type="submit"]');
    const selected = Array.from(form.querySelectorAll('input[name="customer"]:checked'))
      .map(cb => cb.value);
    if (!selected.length) return;
    statusContainer.innerHTML = '<p style="text-align:center;margin-top:10px;">Odota...</p>';
    if (btn) btn.disabled = true;
    try {
      const resp = await fetch(`/api/asiakasrajapinnat_manual?names=${encodeURIComponent(selected.join(','))}`);
      const items = await resp.json();
      if (btn) btn.disabled = false;
      if (resp.status === 200) {
        const rows = items
          .map(item => 
            `<tr>
              <td>${item.run}</td>
              <td>${item.customer}</td>
              <td>${item.response}</td>
            </tr>`
          )
          .join('');

        const html =
          `<p></p>
          <table>
            <thead>
              <tr><th>Suoritus</th><th>Asiakas</th><th>Tila</th></tr>
            </thead>
            <tbody>
              ${rows}
            </tbody>
          </table>`;
        statusContainer.innerHTML = html;
      } else if (resp.status === 400 && items.trim() === 'invalid_name') {
          statusContainer.innerHTML = '<p style="text-align:center;margin-top:10px;">Virheellinen asiakasnimi.</p>';
      }
    } catch (err) {
      if (btn) btn.disabled = false;
      statusContainer.innerHTML = '<p style="text-align:center;margin-top:10px;">Virhe palveluun yhdistettäessä.</p>';
      console.error(err);
    }
  });
})();
