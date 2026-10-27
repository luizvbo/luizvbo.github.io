// Minimal lightbox for article images using the native <dialog> element.
// Opens on click/Enter; closes on backdrop click, image click, or ESC.
// Images larger than the viewport open fitted to it and are marked zoomable:
// clicking them zooms to 100% centred on the clicked point, then scroll
// (wheel/trackpad/touch) or drag pans; clicking again zooms back out.
document.addEventListener('DOMContentLoaded', () => {
    const imgs = document.querySelectorAll('article img');
    if (!imgs.length) return;

    const dialog = document.createElement('dialog');
    dialog.className = 'lightbox';
    dialog.setAttribute('aria-label', 'Image viewer');
    const big = document.createElement('img');
    big.draggable = false;
    big.tabIndex = 0;
    dialog.appendChild(big);
    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'lightbox-close';
    closeBtn.setAttribute('aria-label', 'Close');
    closeBtn.innerHTML =
        '<svg width="18" height="18" viewBox="0 0 18 18" fill="none" ' +
        'stroke="currentColor" stroke-width="2" stroke-linecap="round" ' +
        'aria-hidden="true"><path d="M4 4l10 10M14 4L4 14"/></svg>';
    dialog.appendChild(closeBtn);
    document.body.appendChild(dialog);

    let drag = null;
    let dragged = false;

    // True when the fitted image is downscaled from its natural size, i.e.
    // zooming would reveal more detail. clientWidth/Height force layout, so
    // this is only meaningful while the dialog is open.
    const isZoomable = () =>
        dialog.open && big.naturalWidth > 0 &&
        (big.naturalWidth > big.clientWidth + 1 ||
         big.naturalHeight > big.clientHeight + 1);

    const updateZoomable = () =>
        dialog.classList.toggle('zoomable', isZoomable());

    // fx/fy are the zoom focus point as fractions of the fitted image.
    const zoom = (fx = 0.5, fy = 0.5) => {
        dialog.classList.add('zoomed');
        dialog.scrollLeft = fx * big.offsetWidth - dialog.clientWidth / 2;
        dialog.scrollTop = fy * big.offsetHeight - dialog.clientHeight / 2;
    };

    const unzoom = () => {
        dialog.classList.remove('zoomed');
        dialog.scrollLeft = dialog.scrollTop = 0;
    };

    const open = (img) => {
        unzoom();
        dragged = false;
        big.src = img.src;
        big.alt = img.alt || '';
        dialog.showModal();
        updateZoomable();
    };

    // For uncached images naturalWidth is not ready at open() time.
    big.addEventListener('load', updateZoomable);

    // Drag to pan while zoomed. Touch pointers fall back to native scrolling
    // (the browser fires pointercancel and scrolls the dialog itself).
    big.addEventListener('pointerdown', (e) => {
        if (e.button !== 0 || !dialog.classList.contains('zoomed')) return;
        drag = { x: e.clientX, y: e.clientY, l: dialog.scrollLeft, t: dialog.scrollTop };
        big.setPointerCapture(e.pointerId);
    });
    big.addEventListener('pointermove', (e) => {
        if (!drag) return;
        const dx = e.clientX - drag.x;
        const dy = e.clientY - drag.y;
        if (dx * dx + dy * dy > 25) dragged = true;
        dialog.scrollLeft = drag.l - dx;
        dialog.scrollTop = drag.t - dy;
    });
    const endDrag = () => { drag = null; };
    big.addEventListener('pointerup', endDrag);
    big.addEventListener('pointercancel', endDrag);

    const activate = (e) => {
        if (dialog.classList.contains('zoomed')) return unzoom();
        if (isZoomable()) {
            if (e.type === 'click') {
                const rect = big.getBoundingClientRect();
                return zoom((e.clientX - rect.left) / rect.width,
                            (e.clientY - rect.top) / rect.height);
            }
            return zoom();
        }
        dialog.close();
    };

    dialog.addEventListener('click', (e) => {
        if (dragged) { // a pan just ended; swallow the trailing click
            dragged = false;
            return;
        }
        if (e.target === dialog) return dialog.close();
        if (e.target === big) activate(e);
    });

    closeBtn.addEventListener('click', () => dialog.close());

    big.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            activate(e);
        }
    });

    imgs.forEach((img) => {
        // Skip linked images, site chrome (avatar), and opt-outs.
        if (img.closest('a') || img.classList.contains('u-photo') || img.classList.contains('no-lightbox')) return;
        img.tabIndex = 0;
        img.setAttribute('role', 'button');
        img.setAttribute('aria-label', 'Open image fullscreen');
        img.addEventListener('click', () => open(img));
        img.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                open(img);
            }
        });
    });
});
