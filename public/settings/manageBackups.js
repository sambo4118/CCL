const MAX_SLOTS = 5;

let currentBackups = {
    automatic: [],
    manual: []
};
let selectedBackup = null;
let pendingLoadBackup = null;
let isInitialized = false;

function formatBytes(bytes) {
    if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB'];
    const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const value = bytes / Math.pow(1024, index);
    return `${value.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

function formatDateParts(timestamp) {
    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) {
        return { dateStr: 'Unknown date', timeStr: '', fullStr: 'Unknown date', relativeStr: '' };
    }

    const dateStr = date.toLocaleDateString(undefined, {
        month: 'short',
        day: 'numeric',
        year: 'numeric'
    });
    const timeStr = date.toLocaleTimeString(undefined, {
        hour: 'numeric',
        minute: '2-digit',
        second: '2-digit'
    });

    const diffSeconds = Math.max(0, Math.floor((Date.now() - date.getTime()) / 1000));
    let relativeStr = 'Just now';
    if (diffSeconds >= 86400) {
        const days = Math.floor(diffSeconds / 86400);
        relativeStr = `${days}d ago`;
    } else if (diffSeconds >= 3600) {
        const hours = Math.floor(diffSeconds / 3600);
        relativeStr = `${hours}h ago`;
    } else if (diffSeconds >= 60) {
        const mins = Math.floor(diffSeconds / 60);
        relativeStr = `${mins}m ago`;
    }

    return {
        dateStr,
        timeStr,
        fullStr: `${dateStr} at ${timeStr}`,
        relativeStr
    };
}

function setBackgroundInert(isInert) {
    const navbar = document.querySelector('nav.navbar');
    const mainContainer = document.querySelector('.container.mt-5');
    [navbar, mainContainer].forEach((el) => {
        if (!el) return;
        if (isInert) {
            el.setAttribute('inert', '');
        } else {
            el.removeAttribute('inert');
        }
    });
}

function showStatus(message, type = 'warning') {
    const banner = document.getElementById('backupStatusBanner');
    if (!banner) return;

    banner.className = `notification is-${type} mb-4 py-3 px-4`;
    banner.textContent = message;
}

function hideStatus() {
    const banner = document.getElementById('backupStatusBanner');
    if (!banner) return;
    banner.classList.add('is-hidden');
    banner.textContent = '';
}

function promptLoadConfirmation(backup) {
    pendingLoadBackup = backup;
    const confirmBanner = document.getElementById('backupConfirmBanner');
    const confirmMessage = document.getElementById('backupConfirmMessage');
    if (!confirmBanner || !confirmMessage) return;

    const { fullStr } = formatDateParts(backup.timestamp);
    const labelType = backup.type === 'automatic' ? 'automatic' : 'manual';
    confirmMessage.textContent = `Load ${labelType} backup from ${fullStr}? This will overwrite current library data.`;
    confirmBanner.classList.remove('is-hidden');

    const confirmBtn = document.getElementById('confirmLoadBackupButton');
    confirmBtn?.focus();
}

function hideLoadConfirmation() {
    pendingLoadBackup = null;
    const confirmBanner = document.getElementById('backupConfirmBanner');
    if (!confirmBanner) return;
    confirmBanner.classList.add('is-hidden');
}

function updateSelectedUI() {
    const loadSelectedBtn = document.getElementById('loadSelectedBackupButton');
    if (loadSelectedBtn) {
        loadSelectedBtn.disabled = !selectedBackup;
    }

    const allCards = document.querySelectorAll('#manageBackupsModal [data-backup-filename]');
    allCards.forEach((card) => {
        const isSelected = selectedBackup && card.dataset.backupFilename === selectedBackup.filename;
        card.style.border = isSelected
            ? '2px solid hsl(var(--bulma-warning-h), var(--bulma-warning-s), var(--bulma-warning-l))'
            : '2px solid rgba(255, 255, 255, 0.08)';
        card.setAttribute('aria-current', isSelected ? 'true' : 'false');
    });
}

function createBackupSlotElement(backup, index, rowType) {
    const li = document.createElement('li');
    li.className = 'column is-one-fifth-desktop is-half-tablet is-full-mobile';

    const card = document.createElement('div');
    card.className = 'box p-3 is-flex is-flex-direction-column is-justify-content-space-between';
    card.style.height = '100%';

    if (!backup) {
        card.style.border = '2px dashed rgba(255, 255, 255, 0.12)';
        card.style.opacity = '0.55';

        const emptyHeader = document.createElement('div');
        emptyHeader.className = 'is-flex is-justify-content-space-between is-align-items-center mb-2';

        const slotBadge = document.createElement('span');
        slotBadge.className = 'tag is-dark is-small';
        slotBadge.textContent = `${rowType === 'automatic' ? 'Auto' : 'Manual'} #${index + 1}`;
        emptyHeader.appendChild(slotBadge);

        const emptyBody = document.createElement('p');
        emptyBody.className = 'is-size-7 has-text-grey has-text-centered my-4';
        emptyBody.textContent = 'Empty slot';

        card.append(emptyHeader, emptyBody);
        li.appendChild(card);
        return li;
    }

    const { dateStr, timeStr, fullStr, relativeStr } = formatDateParts(backup.timestamp);
    const typeLabel = rowType === 'automatic' ? 'Auto' : 'Manual';

    card.dataset.backupFilename = backup.filename;
    card.style.cursor = 'pointer';
    card.style.transition = 'border-color 0.15s ease, transform 0.15s ease';
    card.style.border = '2px solid rgba(255, 255, 255, 0.08)';

    card.addEventListener('click', () => {
        selectedBackup = backup;
        updateSelectedUI();
    });

    const topSection = document.createElement('div');

    const headerRow = document.createElement('div');
    headerRow.className = 'is-flex is-justify-content-space-between is-align-items-center mb-2';

    const indexTag = document.createElement('span');
    indexTag.className = 'tag is-warning has-text-white is-small';
    indexTag.textContent = `${typeLabel} #${index + 1}`;
    headerRow.appendChild(indexTag);

    if (index === 0) {
        const latestTag = document.createElement('span');
        latestTag.className = 'tag is-warning is-light is-small';
        latestTag.textContent = 'Latest';
        headerRow.appendChild(latestTag);
    } else {
        const relSpan = document.createElement('span');
        relSpan.className = 'is-size-7 has-text-grey-light';
        relSpan.textContent = relativeStr;
        headerRow.appendChild(relSpan);
    }

    const dateP = document.createElement('p');
    dateP.className = 'has-text-weight-semibold is-size-6 mb-0';
    dateP.textContent = dateStr;

    const timeP = document.createElement('p');
    timeP.className = 'is-size-7 has-text-grey-light mb-2';
    timeP.textContent = timeStr;

    const metaP = document.createElement('p');
    metaP.className = 'is-size-7 has-text-warning mb-3';
    metaP.textContent = `${formatBytes(backup.size)} • ${relativeStr}`;

    topSection.append(headerRow, dateP, timeP, metaP);

    const loadBtn = document.createElement('button');
    loadBtn.type = 'button';
    loadBtn.className = 'button is-small is-warning has-text-white is-fullwidth';
    loadBtn.setAttribute('aria-label', `Load ${rowType} backup from ${fullStr}`);

    const iconSpan = document.createElement('span');
    iconSpan.className = 'icon is-small';
    const icon = document.createElement('i');
    icon.className = 'fa-solid fa-rotate-left';
    icon.setAttribute('aria-hidden', 'true');
    iconSpan.appendChild(icon);

    const textSpan = document.createElement('span');
    textSpan.textContent = 'Load Backup';

    loadBtn.append(iconSpan, textSpan);
    loadBtn.addEventListener('click', (event) => {
        event.stopPropagation();
        selectedBackup = backup;
        updateSelectedUI();
        promptLoadConfirmation(backup);
    });

    card.append(topSection, loadBtn);
    li.appendChild(card);
    return li;
}

function renderBackupRows() {
    const autoRow = document.getElementById('automaticBackupsRow');
    const manualRow = document.getElementById('manualBackupsRow');
    const autoCountTag = document.getElementById('autoBackupCountTag');
    const manualCountTag = document.getElementById('manualBackupCountTag');

    if (autoCountTag) {
        autoCountTag.textContent = `${currentBackups.automatic.length} / ${MAX_SLOTS}`;
    }
    if (manualCountTag) {
        manualCountTag.textContent = `${currentBackups.manual.length} / ${MAX_SLOTS}`;
    }

    if (autoRow) {
        autoRow.innerHTML = '';
        for (let i = 0; i < MAX_SLOTS; i++) {
            const backup = currentBackups.automatic[i] || null;
            autoRow.appendChild(createBackupSlotElement(backup, i, 'automatic'));
        }
    }

    if (manualRow) {
        manualRow.innerHTML = '';
        for (let i = 0; i < MAX_SLOTS; i++) {
            const backup = currentBackups.manual[i] || null;
            manualRow.appendChild(createBackupSlotElement(backup, i, 'manual'));
        }
    }

    if (selectedBackup) {
        const allAvailable = [...currentBackups.automatic, ...currentBackups.manual];
        const match = allAvailable.find((b) => b.filename === selectedBackup.filename);
        selectedBackup = match || null;
    }

    updateSelectedUI();
}

async function fetchBackups() {
    const response = await fetch('/api/backups', { credentials: 'include' });
    if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        throw new Error(err.error || `Failed to load backups (${response.status})`);
    }

    const data = await response.json();
    currentBackups = {
        automatic: Array.isArray(data.automatic) ? data.automatic.slice(0, MAX_SLOTS) : [],
        manual: Array.isArray(data.manual) ? data.manual.slice(0, MAX_SLOTS) : []
    };

    const scheduleText = document.getElementById('autoBackupScheduleText');
    if (scheduleText && data.intervalMinutes) {
        scheduleText.textContent = `Runs every ${data.intervalMinutes} min (keeps past ${MAX_SLOTS})`;
    }

    renderBackupRows();
}

async function handleMakeBackup() {
    const makeBtn = document.getElementById('makeBackupButton');
    if (!makeBtn) return;

    hideLoadConfirmation();
    hideStatus();
    makeBtn.classList.add('is-loading');
    makeBtn.disabled = true;

    try {
        const response = await fetch('/api/backups', {
            method: 'POST',
            credentials: 'include'
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.error || 'Failed to create backup');
        }

        const data = await response.json();
        currentBackups = {
            automatic: Array.isArray(data.automatic) ? data.automatic.slice(0, MAX_SLOTS) : [],
            manual: Array.isArray(data.manual) ? data.manual.slice(0, MAX_SLOTS) : []
        };

        if (data.backup) {
            selectedBackup = data.backup;
        }

        renderBackupRows();
        showStatus('New manual backup created successfully.', 'warning');
    } catch (err) {
        console.error('Error creating backup:', err);
        showStatus(err.message || 'Failed to create backup.', 'danger');
    } finally {
        makeBtn.classList.remove('is-loading');
        makeBtn.disabled = false;
    }
}

async function handleConfirmLoadBackup() {
    if (!pendingLoadBackup) return;

    const backupToLoad = pendingLoadBackup;
    const confirmBtn = document.getElementById('confirmLoadBackupButton');
    if (confirmBtn) {
        confirmBtn.classList.add('is-loading');
        confirmBtn.disabled = true;
    }

    try {
        const response = await fetch('/api/backups/load', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'include',
            body: JSON.stringify({ filename: backupToLoad.filename })
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.error || 'Failed to load backup');
        }

        const data = await response.json();
        currentBackups = {
            automatic: Array.isArray(data.automatic) ? data.automatic.slice(0, MAX_SLOTS) : [],
            manual: Array.isArray(data.manual) ? data.manual.slice(0, MAX_SLOTS) : []
        };

        hideLoadConfirmation();
        renderBackupRows();

        const { fullStr } = formatDateParts(backupToLoad.timestamp);
        showStatus(`Loaded ${backupToLoad.type} backup from ${fullStr} successfully.`, 'warning');
    } catch (err) {
        console.error('Error loading backup:', err);
        hideLoadConfirmation();
        showStatus(err.message || 'Failed to load backup.', 'danger');
    } finally {
        if (confirmBtn) {
            confirmBtn.classList.remove('is-loading');
            confirmBtn.disabled = false;
        }
    }
}

function openManageBackupsModal() {
    const modal = document.getElementById('manageBackupsModal');
    if (!modal) return;

    hideStatus();
    hideLoadConfirmation();
    modal.classList.add('is-active');
    setBackgroundInert(true);

    const makeBtn = document.getElementById('makeBackupButton');
    makeBtn?.focus();

    fetchBackups().catch((err) => {
        console.error('Failed to fetch backups:', err);
        showStatus(err.message || 'Unable to load available backups.', 'danger');
    });
}

function closeManageBackupsModal() {
    const modal = document.getElementById('manageBackupsModal');
    if (!modal) return;

    modal.classList.remove('is-active');
    setBackgroundInert(false);
    hideLoadConfirmation();

    const triggerBtn = document.getElementById('manageBackupsButton');
    triggerBtn?.focus();
}

export function setupManageBackups() {
    const triggerButton = document.getElementById('manageBackupsButton');
    const modal = document.getElementById('manageBackupsModal');
    if (!triggerButton || !modal) return;

    if (!isInitialized) {
        isInitialized = true;

        triggerButton.addEventListener('click', openManageBackupsModal);

        const closeBtn = document.getElementById('closeManageBackupsModal');
        const footerCloseBtn = document.getElementById('closeManageBackupsFooterButton');
        const backdrop = document.getElementById('manageBackupsBackdrop');

        closeBtn?.addEventListener('click', closeManageBackupsModal);
        footerCloseBtn?.addEventListener('click', closeManageBackupsModal);
        backdrop?.addEventListener('click', closeManageBackupsModal);

        document.addEventListener('keydown', (event) => {
            if (event.key === 'Escape' && modal.classList.contains('is-active')) {
                closeManageBackupsModal();
            }
        });

        const makeBackupBtn = document.getElementById('makeBackupButton');
        makeBackupBtn?.addEventListener('click', handleMakeBackup);

        const loadSelectedBtn = document.getElementById('loadSelectedBackupButton');
        loadSelectedBtn?.addEventListener('click', () => {
            if (selectedBackup) {
                promptLoadConfirmation(selectedBackup);
            }
        });

        const confirmLoadBtn = document.getElementById('confirmLoadBackupButton');
        confirmLoadBtn?.addEventListener('click', handleConfirmLoadBackup);

        const cancelLoadBtn = document.getElementById('cancelLoadBackupButton');
        cancelLoadBtn?.addEventListener('click', hideLoadConfirmation);
    }
}
