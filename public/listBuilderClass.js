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
        editConfig = null,
        columnsConfig = {}
    }) {

        this.container = typeof container === 'string'
            ? document.querySelector(container) : container;

        
        this.baseColor = baseColor;
        this.emptyMessage = emptyMessage;
        this.imageConfig = imageConfig;
        this.columnsConfig = columnsConfig;
        this.actionConfig = actionConfig;
        this.editConfig = editConfig;
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
        rowWrapper.className = 'columns is-gapless is-mobile mb-4 viewlist-row';
        // 2. Main content column (left side)
        const mainCol = document.createElement('div');
        mainCol.className = `column is-${mainColSize} has-background-${this.baseColor} p-4 viewlist-main`;
        mainCol.style.borderRadius = hasAction ? '0.5rem 0 0 0.5rem' : '0.5rem';
        const mediaElement = document.createElement('article');
        mediaElement.className = 'media viewlist-media';
        // Optional left image (e.g. Book Cover)
        if (this.imageConfig) {
            mediaElement.appendChild(this._renderMediaLeft(item, this.imageConfig));
        }
        // Inner columns container
        const mediaContent = document.createElement('div');
        mediaContent.className = 'media-content viewlist-media-content';
        const content = document.createElement('div');
        content.className = 'content';
        const innerColumns = document.createElement('div');
        innerColumns.className = 'columns is-mobile viewlist-inner-columns';
        const colEntries = Object.entries(this.columnsConfig);
        colEntries.forEach(([key, colData], index) => {
            const isRightmostCol = colEntries.length > 1 && index === colEntries.length - 1;
            const colEl = this._renderContentColumn(key, colData, item, isRightmostCol);
            innerColumns.appendChild(colEl);
        });
        
        content.appendChild(innerColumns);
        mediaContent.appendChild(content);
        mediaElement.appendChild(mediaContent);

        if (this.editConfig && colEntries.length <= 1) {
            const editBtn = this._renderEditButton(item, this.editConfig);
            if (editBtn) {
                const mediaRight = document.createElement('div');
                mediaRight.className = 'media-right';
                mediaRight.appendChild(editBtn);
                mediaElement.appendChild(mediaRight);
            }
        }

        mainCol.appendChild(mediaElement);
        rowWrapper.appendChild(mainCol);

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
        mediaLeft.className = 'media-left viewlist-media-left';
        const imgContainer = document.createElement('p');
        imgContainer.className = 'image ml-4 mt-4 mb-4 viewlist-cover';
        imgContainer.style.width = width;
        imgContainer.style.height = height;
        const img = document.createElement('img');
        img.src = src || placeholder;
        img.alt = alt;
        img.loading = 'lazy';
        img.style.width = width;
        img.style.height = height;
        img.style.aspectRatio = '3 / 4';
        img.style.objectFit = 'fill';
        img.addEventListener('error', () => {
            if (img.src !== placeholder) img.src = placeholder;
        });
        imgContainer.appendChild(img);
        mediaLeft.appendChild(imgContainer);
        return mediaLeft;
    }
    _renderContentColumn(key, colData, item, isRightmostCol = false) {
        const size = colData.size ?? 4;
        const align = colData.align ?? 'left'; // 'left' | 'centered' | 'right'
        const col = document.createElement('div');
        col.className = `column is-${size} has-text-${align} viewlist-col viewlist-col-${key} ${colData.className ?? ''}`.trim();

        const editBtn = (isRightmostCol && this.editConfig)
            ? this._renderEditButton(item, this.editConfig)
            : null;
        if (editBtn) {
            col.appendChild(editBtn);
        }

        const details = document.createElement('div');
        details.className = `content viewlist-details ${editBtn ? 'mt-2' : 'mt-4'} ${colData.contentClassName ?? ''}`.trim();
        const lines = typeof colData.lines === 'function' ? colData.lines(item) : (colData.lines ?? []);
        lines.forEach((lineConfig) => {
            const lineEl = this._renderLine(lineConfig, item);
            if (lineEl) details.appendChild(lineEl);
        });
        col.appendChild(details);
        return col;
    }

    _renderEditButton(item, editConfig = this.editConfig) {
        if (!editConfig) return null;

        const config = typeof editConfig === 'function' ? { onClick: editConfig } : editConfig;
        if (typeof config.show === 'function' && !config.show(item)) return null;
        if (config.show === false) return null;

        const wrapper = document.createElement('div');
        wrapper.className = 'is-flex is-justify-content-flex-end mb-2 viewlist-edit-wrapper';

        const btn = document.createElement('button');
        btn.type = 'button';

        const rawColor = config.buttonColor ?? config.color;
        const buttonColor = typeof rawColor === 'function' ? rawColor(item) : rawColor;
        btn.className = `button viewlist-edit-btn ${buttonColor ? `is-${buttonColor}` : ''} ${config.className ?? ''}`.trim();

        if (config.title) {
            btn.title = typeof config.title === 'function' ? config.title(item) : config.title;
        }

        const iconClass = typeof config.iconClass === 'function'
            ? config.iconClass(item)
            : (config.iconClass ?? 'fas fa-solid fa-pen-to-square');
        if (iconClass) {
            const iconSpan = document.createElement('span');
            iconSpan.className = 'icon';
            const icon = document.createElement('i');
            icon.className = iconClass;
            iconSpan.appendChild(icon);
            btn.appendChild(iconSpan);
        }

        const textValue = typeof config.text === 'function' ? config.text(item) : config.text;
        if (textValue) {
            const textSpan = document.createElement('span');
            textSpan.textContent = textValue;
            btn.appendChild(textSpan);
        }

        if (typeof config.onClick === 'function') {
            btn.addEventListener('click', (e) => config.onClick(item, e));
        }

        wrapper.appendChild(btn);
        return wrapper;
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
        rightCol.className = `column is-${size} has-background-${bgColor} p-2 is-flex is-justify-content-center is-align-items-center viewlist-action`;
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