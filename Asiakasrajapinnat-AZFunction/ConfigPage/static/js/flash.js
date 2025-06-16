document.addEventListener('DOMContentLoaded', () => {
// event-delegate on the container for future flashes too
document.getElementById('flash-container')?.addEventListener('click', e => {
    if (e.target.classList.contains('flash-close')) {
    const flash = e.target.closest('.flash');
    flash?.remove();
    }
});
});
