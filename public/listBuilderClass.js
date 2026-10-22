// config = {
//      right: {
//          size: number,
//      },
//      center: {
//          size: number,
//      },
//      left: {
//          size: number,
//      }
// }
export class Viewlist {
    constructor({
        container,
        baseColor = 'primary',
        emptyMessage = 'No items found.',
        imageConfig = null,
        actionConfig = null,
        columnsConfig = {}
    }) {

        this.container = typeof container === 'string'
            ? document.querySelector(container) : container;

        
        this.baseColor = baseColor;
        this.emptyMessage = emptyMessage;
        this.imageConfig = imageConfig;
        this.columnsConfig = columnsConfig;
        this.actionConfig = actionConfig;
        this.items = [];
        this.rows = [];
    }

    setColumns(columnsConfig) {
        this.columnsConfig = { ...this.columnsConfig, ...columnsConfig };
    }

    render(items = []) {
        this.items = items;
        this.rows = [];

        if (!this.container) console.error(`List is missing a defined container`);
        this.container.innerHTML = '';

        if (items.length === 0) return this.container.innerHTML = `<p class="has-text-grey has-text-centered py-4">${this.emptyMessage}</p>`;

        items.forEach((item) => {
            const rowElement = this._createRowElement(item);
            if (rowElement) {
                this.rows.push(rowElement);
                this.container.appendChild(rowElement);
            }
        });
    }

    clear() {
        this.items = [];
        this.rows = [];
        if (this.container) this.container.innerHTML = '';
    }

    _createRowElement(item) {
        if (!item) return null;
        const hasAction = Boolean(this.actionConfig);
        const mainColSize = hasAction ? (12 - (this.actionConfig.size ?? 1)) : 12;
        // 1. Outer row wrapper
        const rowWrapper = document.createElement('div');
        rowWrapper.className = 'columns is-gapless is-mobile mb-4';
        // 2. Main content column (left side)
        const mainCol = document.createElement('div');
        mainCol.className = `column is-${mainColSize} has-background-${this.baseColor} p-4`;
        mainCol.style.borderRadius = hasAction ? '0.5rem 0 0 0.5rem' : '0.5rem';
        const mediaElement = document.createElement('article');
        mediaElement.className = 'media';
        // Optional left image (e.g. Book Cover)
        if (this.imageConfig) {
            mediaElement.appendChild(this._renderMediaLeft(item, this.imageConfig));
        }
        // Inner columns container
        const mediaContent = document.createElement('div');
        mediaContent.className = 'media-content';
        const content = document.createElement('div');
        content.className = 'content';
        const innerColumns = document.createElement('div');
        innerColumns.className = 'columns is-mobile';
        Object.entries(this.columnsConfig).forEach(([key, colData]) => {
            const colEl = this._renderContentColumn(key, colData, item);
            innerColumns.appendChild(colEl);
        });
        content.appendChild(innerColumns);
        mediaContent.appendChild(content);
        mediaElement.appendChild(mediaContent);
        mainCol.appendChild(mediaElement);
        rowWrapper.appendChild(mainCol);
        // 3. Optional action column (right side button strip)
        if (hasAction) {
            rowWrapper.appendChild(this._renderActionColumn(item, this.actionConfig));
        }
        return rowWrapper;
    }
    
    _renderMediaLeft(item, imgConfig) {
        const placeholder = imgConfig.placeholder ?? '/images/128x128.png';
        const width = imgConfig.width ?? '96px';
        const height = imgConfig.height ?? '128px';
        const src = typeof imgConfig.src === 'function' ? imgConfig.src(item) : item[imgConfig.srcKey];
        const alt = typeof imgConfig.alt === 'function' ? imgConfig.alt(item) : (imgConfig.alt ?? 'Cover image');
        const mediaLeft = document.createElement('figure');
        mediaLeft.className = 'media-left';
        const imgContainer = document.createElement('p');
        imgContainer.className = 'image ml-4 mt-4 mb-4';
        imgContainer.style.width = width;
        imgContainer.style.height = height;
        const img = document.createElement('img');
        img.src = src || placeholder;
        img.alt = alt;
        img.loading = 'lazy';
        img.style.width = width;
        img.style.height = height;
        img.style.objectFit = 'fill';
        img.addEventListener('error', () => {
            if (img.src !== placeholder) img.src = placeholder;
        });
        imgContainer.appendChild(img);
        mediaLeft.appendChild(imgContainer);
        return mediaLeft;
    }
        _renderContentColumn(key, colData, item) {
        const size = colData.size ?? 4;
        const align = colData.align ?? 'left'; // 'left' | 'centered' | 'right'
        const col = document.createElement('div');
        col.className = `column is-${size} has-text-${align} ${colData.className ?? ''}`.trim();
        const details = document.createElement('div');
        details.className = `content mt-4 ${colData.contentClassName ?? ''}`.trim();
        // Each column has a `lines` array of line configs
        const lines = typeof colData.lines === 'function' ? colData.lines(item) : (colData.lines ?? []);
        lines.forEach((lineConfig) => {
            const lineEl = this._renderLine(lineConfig, item);
            if (lineEl) details.appendChild(lineEl);
        });
        col.appendChild(details);
        return col;
    }

    _renderLine(lineConfig, item) {
        const textValue = typeof lineConfig.text === 'function'
            ? lineConfig.text(item)
            : (item[lineConfig.key] ?? lineConfig.defaultText ?? '');
        const href = typeof lineConfig.href === 'function'
            ? lineConfig.href(item)
            : lineConfig.href;
        switch (lineConfig.type) {
            case 'title': {
                const p = document.createElement('p');
                p.className = lineConfig.className ?? 'title is-4';
                if (href) {
                    const a = document.createElement('a');
                    a.href = href;
                    a.className = lineConfig.linkClassName ?? 'has-text-white';
                    a.textContent = textValue;
                    p.appendChild(a);
                } else {
                    p.textContent = textValue;
                }
                return p;
            }
            case 'stackedLink': {
                // Splits text by whitespace into stacked <p> lines inside an <a> (like studentName)
                const a = document.createElement('a');
                a.href = href ?? '#';
                a.className = 'is-block';
                const parts = String(textValue).trim().split(/\s+/);
                parts.forEach((part) => {
                    const p = document.createElement('p');
                    p.className = lineConfig.className ?? 'subtitle is-6 has-text-white';
                    p.textContent = part;
                    a.appendChild(p);
                });
                return a;
            }
            case 'subtitle':
            default: {
                const p = document.createElement('p');
                p.className = lineConfig.className ?? 'subtitle is-6 has-text-grey';
                p.textContent = textValue;
                return p;
            }
        }
    }

    _renderActionColumn(item, actionConfig) {
        const size = actionConfig.size ?? 1;
        const bgColor = typeof actionConfig.color === 'function'
            ? actionConfig.color(item)
            : (actionConfig.color ?? 'warning');
        const buttonColor = typeof actionConfig.buttonColor === 'function'
            ? actionConfig.buttonColor(item)
            : (actionConfig.buttonColor ?? 'danger');
        const rightCol = document.createElement('div');
        rightCol.className = `column is-${size} has-background-${bgColor} p-2 is-flex is-justify-content-center is-align-items-center`;
        rightCol.style.borderRadius = '0 0.5rem 0.5rem 0';
        const btn = document.createElement('button');
        btn.className = `button is-${buttonColor} is-fullwidth has-text-white has-text-weight-extra-bold`;
        btn.style.height = '100%';
        btn.style.width = '100%';
        btn.innerHTML = `<span class="icon is-medium"><i class="${actionConfig.iconClass ?? 'fas fa-check fa-lg'}"></i></span>`;
        if (actionConfig.onClick) {
            btn.addEventListener('click', (e) => actionConfig.onClick(item, e));
        }
        rightCol.appendChild(btn);
        return rightCol;
    }
}