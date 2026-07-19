document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const eventList = document.getElementById('eventList');
    const diffFrame = document.getElementById('diffFrame');
    const welcomeMsg = document.getElementById('welcomeMessage');
    const appLayout = document.querySelector('.app-layout');

    // Header Elements
    const currentWiki = document.getElementById('currentWiki');
    const currentTitle = document.getElementById('currentTitle');
    const mobileBackBtn = document.getElementById('mobileBackBtn');
    const diffActions = document.getElementById('diffActions');
    const connectionStatus = document.getElementById('connectionStatus');

    // Controls
    const pauseBtn = document.getElementById('pauseBtn');
    const resumeBtn = document.getElementById('resumeBtn');
    const filterBtn = document.getElementById('filterBtn');
    const filterModal = new bootstrap.Modal(document.getElementById('filterModal'));
    const saveFilterBtn = document.getElementById('saveFilterBtn');
    const filterCode = document.getElementById('filterCode');

    let eventSource = null;
    let selectedEvent = null;
    let retryDelayMs = 3000;
    const MIN_RETRY_DELAY_MS = 3000;
    const MAX_RETRY_DELAY_MS = 60000;

    // --- Connection health ---
    let lastActivityAt = 0;
    let staleTimeoutMs = 45000;
    const HEALTH_CHECK_MS = 1000;

    function setConnected(connected) {
        if (!connectionStatus) return;
        connectionStatus.classList.toggle('is-connected', connected);
        connectionStatus.classList.toggle('is-disconnected', !connected);
        const label = connected ? 'Live' : 'Disconnected';
        connectionStatus.title = label;
        connectionStatus.setAttribute('aria-label', 'Connection status: ' + label);
        const labelEl = connectionStatus.querySelector('.connection-status-label');
        if (labelEl) labelEl.textContent = label;
    }

    function markAlive(intervalMs) {
        lastActivityAt = Date.now();
        if (intervalMs > 0) staleTimeoutMs = Math.max(intervalMs * 3, 20000);
        setConnected(true);
    }

    function reconnect() {
        if (eventSource) {
            try { eventSource.close(); } catch (e) { /* ignore */ }
            eventSource = null;
        }
        setConnected(false);
        lastActivityAt = Date.now(); // give the fresh connection a full window
        const delay = retryDelayMs + Math.random() * (retryDelayMs / 2);
        setTimeout(connect, delay);
        retryDelayMs = Math.min(retryDelayMs * 2, MAX_RETRY_DELAY_MS);
    }

    function isTypingTarget(target) {
        if (!target) return false;
        const tag = target.tagName;
        return target.isContentEditable || tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';
    }

    function getActiveTheme() {
        return document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
    }

    function applyDiffFrameTheme() {
        const doc = diffFrame && (diffFrame.contentDocument || diffFrame.contentWindow?.document);
        if (!doc || !doc.body) return;

        const dark = getActiveTheme() === 'dark';
        doc.body.style.background = dark ? '#0f172a' : '#ffffff';
        doc.body.style.color = dark ? '#e2e8f0' : '#0f172a';

        if (doc.head) {
            let styleEl = doc.getElementById('wikimonitorDiffTheme');
            if (!styleEl) {
                styleEl = doc.createElement('style');
                styleEl.id = 'wikimonitorDiffTheme';
                doc.head.appendChild(styleEl);
            }
            styleEl.textContent = dark
                ? `
                    table.diff td.diff-context { background: #111827; color: #e2e8f0; }
                    table.diff td.diff-marker { background: #1f2937; color: #93c5fd; }
                    table.diff td.diff-deletedline { background: #3f1d1d; color: #fecaca; }
                    table.diff td.diff-addedline { background: #0f2e1e; color: #bbf7d0; }
                  `
                : `
                    table.diff td.diff-context { background: #ffffff; color: #0f172a; }
                    table.diff td.diff-marker { background: #f8fafc; color: #2563eb; }
                    table.diff td.diff-deletedline { background: #fee2e2; color: #7f1d1d; }
                    table.diff td.diff-addedline { background: #dcfce7; color: #14532d; }
                  `;
        }
    }

    document.addEventListener('themechange', applyDiffFrameTheme);

    function getCookie(name) {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        if (parts.length === 2) return parts.pop().split(';').shift();
    }

    function escapeHtml(text) {
        if (!text) return text;
        return String(text)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#039;");
    }

    // --- Page Title ---
    // The DOM keeps at most MAX_VISIBLE cards at a time. New cards are added at
    // the top; once the count exceeds MAX_VISIBLE the oldest card is dropped from
    // the bottom. The title shows "100+" as soon as the list hits the cap.
    const BASE_TITLE = 'WikiMonitor';
    const MAX_VISIBLE = 100;

    function updatePageTitle() {
        if (typeof eventList === 'undefined' || !eventList) return;
        const count = eventList.children.length;
        const display = count > MAX_VISIBLE ? `${MAX_VISIBLE}+` : count;
        document.title = count > 0 ? `${BASE_TITLE} (${display})` : BASE_TITLE;
    }

    // --- SSE Connection ---
    function connect() {
        if (eventSource) return;
        eventSource = new EventSource('/stream');

        eventSource.onopen = () => {
            markAlive();
            retryDelayMs = MIN_RETRY_DELAY_MS; // reset backoff after a good connection
        };

        eventSource.onmessage = (event) => {
            markAlive();
            addEventToQueue(JSON.parse(event.data));
        };

        // Keepalive ping: proves the stream is live even when no edits match, and
        // advertises the server's heartbeat interval so we can size the timeout.
        eventSource.addEventListener('heartbeat', (event) => {
            markAlive(parseInt(event.data, 10) || 0);
        });

        eventSource.onerror = (err) => {
            console.error('EventSource failed:', err);
            setConnected(false);
            // Browser auto-reconnects on its own (readyState CONNECTING); only step
            // in when it has given up.
            if (eventSource && eventSource.readyState === EventSource.CLOSED) {
                reconnect();
            }
        };
    }

    // The once-per-second health check: a silent hang produces no onerror, so if
    // the stream has gone quiet past the stale threshold, treat it as dead.
    setInterval(() => {
        if (!eventSource || !lastActivityAt) return;
        if (Date.now() - lastActivityAt > staleTimeoutMs) {
            console.warn(`SSE stale: no data/heartbeat in ${staleTimeoutMs}ms, reconnecting`);
            reconnect();
        }
    }, HEALTH_CHECK_MS);

    function addEventToQueue(event) {
        if (!event.flagged) return; // Only show flagged items based on previous logic

        // Create Card
        const card = document.createElement('div');
        card.className = 'event-card flagged-card'; // Default to flagged style

        // Calc Diff
        const newLen = (event.length && event.length.new) || 0;
        const oldLen = (event.length && event.length.old) || 0;
        const diff = newLen - oldLen;
        const diffStr = diff > 0 ? `+${diff}` : `${diff}`;
        const diffClass = diff > 0 ? 'text-diff-pos' : (diff < 0 ? 'text-diff-neg' : 'text-diff-neu');

        // Format Time
        const time = new Date(event.timestamp * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });

        card.innerHTML = `
            <div class="d-flex justify-content-between mb-1">
                <span class="badge bg-secondary" style="font-size: 0.7em;">${event.wiki}</span>
                <small class="text-muted" style="font-size: 0.75em;">${time}</small>
            </div>
            <div class="card-title text-dark">${escapeHtml(event.title)}</div>
            <div class="card-meta mt-1">
                <span class="${diffClass}">${diffStr}</span>
                <span class="text-truncate ms-2" style="max-width: 150px;">${escapeHtml(event.user)}</span>
            </div>
            <div class="small text-muted mt-1 text-truncate">${escapeHtml(event.comment || '')}</div>
        `;

        // Actions Footer
        const actionsDiv = document.createElement('div');
        actionsDiv.className = 'card-actions mt-2 d-flex justify-content-between align-items-center pt-2 border-top';
        actionsDiv.style.borderColor = 'rgba(0,0,0,0.05)';

        // Links
        const linksDiv = document.createElement('div');
        linksDiv.className = 'd-flex gap-2';

        const pageLink = `https://${event.server_name}/wiki/${encodeURIComponent(event.title)}`;
        const userLink = `https://${event.server_name}/wiki/Special:Contributions/${encodeURIComponent(event.user)}`;

        linksDiv.innerHTML = `
            <a href="${pageLink}" target="_blank" class="btn btn-xs btn-outline-secondary" title="View Article" onclick="event.stopPropagation()">
                <i class="bi bi-file-text"></i>
            </a>
            <a href="${userLink}" target="_blank" class="btn btn-xs btn-outline-secondary" title="User Contributions" onclick="event.stopPropagation()">
                <i class="bi bi-person"></i>
            </a>
        `;

        // Dismiss Button
        const dismissBtn = document.createElement('button');
        dismissBtn.className = 'btn btn-xs btn-outline-danger';
        dismissBtn.innerHTML = '<i class="bi bi-check-lg"></i>';
        dismissBtn.title = 'Mark as Viewed';
        dismissBtn.onclick = (e) => {
            e.stopPropagation();
            card.remove();
            updatePageTitle();
        };

        actionsDiv.appendChild(linksDiv);
        actionsDiv.appendChild(dismissBtn);

        // Matched Filters section (above actions footer)
        if (event.matchedFilters && event.matchedFilters.length > 0) {
            const filtersDiv = document.createElement('div');
            filtersDiv.className = 'matched-filters pt-2 pb-1';
            // Deduplicate using Set
            const uniqueNames = [...new Set(event.matchedFilters)];
            filtersDiv.innerHTML = '<small class="text-muted me-1"><i class="bi bi-funnel-fill"></i> Matched by:</small>'
                + uniqueNames.map(name =>
                    `<span class="badge bg-warning text-dark me-1">${escapeHtml(name)}</span>`
                ).join('');
            card.appendChild(filtersDiv);
        }

        card.appendChild(actionsDiv);

        // Click Handler (Load Diff)
        card.addEventListener('click', (e) => {
            // Ignore if clicking links (handled by stopPropagation, but good to be safe)
            if (e.target.tagName === 'A' || e.target.closest('a') || e.target.closest('button')) return;

            // Remove active from others
            document.querySelectorAll('.event-card.active').forEach(c => c.classList.remove('active'));
            card.classList.add('active');

            loadDiff(event);
        });

        // Prepend to list
        eventList.insertBefore(card, eventList.firstChild);

        // Limit list size (keep DOM light)
        while (eventList.children.length > MAX_VISIBLE) {
            eventList.removeChild(eventList.lastChild);
        }

        updatePageTitle();
    }

    function loadDiff(event) {
        selectedEvent = event;

        // Update Header
        currentWiki.textContent = event.wiki;
        currentTitle.textContent = event.title;

        // Update Actions
        if (diffActions) {
            diffActions.innerHTML = '';
            if (typeof isLoggedIn !== 'undefined' && isLoggedIn) {
                const undoBtn = document.createElement('button');
                undoBtn.className = 'btn btn-sm btn-outline-warning';
                undoBtn.innerHTML = '<i class="bi bi-arrow-counterclockwise"></i> Undo';
                undoBtn.title = 'Undo (U)';
                undoBtn.onclick = () => performUndo(event);
                diffActions.appendChild(undoBtn);

                const rollbackBtn = document.createElement('button');
                rollbackBtn.className = 'btn btn-sm btn-outline-danger';
                rollbackBtn.innerHTML = '<i class="bi bi-rewind-fill"></i> Rollback';
                rollbackBtn.title = 'Rollback (R)';
                rollbackBtn.onclick = () => performRollback(event);
                diffActions.appendChild(rollbackBtn);
            }
        }

        // Show Iframe, Hide Welcome
        welcomeMsg.classList.add('d-none');
        diffFrame.classList.remove('d-none');

        // Mobile: Slide to Diff View
        appLayout.classList.add('showing-diff');

        // Load Content
        // Construct the internal API URL for diff
        // Note: Reusing the existing /api/diff endpoint
        const server = event.server_name;
        const oldRev = event.revision?.old || 0;
        const newRev = event.revision?.new;

        if (!server || !newRev) {
            diffFrame.srcdoc = '<div style="padding:20px; color:red;">Invalid revision data</div>';
            return;
        }

        // We use srcdoc or direct fetch to inject styles? 
        // Let's use the fetch approach from before but inject into the iframe document
        // Actually, setting src to a controller endpoint that returns HTML is cleaner if we had one.
        // But our /api/diff returns the table HTML fragment.
        // We can write to the doc.

        const doc = diffFrame.contentDocument || diffFrame.contentWindow.document;
        doc.body.innerHTML = '<div style="padding:20px; font-family:sans-serif;">Loading...</div>';

        fetch(`/api/diff?server=${server}&old=${oldRev}&new=${newRev}`)
            .then(res => res.text())
            .then(html => {
                const dark = getActiveTheme() === 'dark';
                const head = `
                    <head>
                        <link rel="stylesheet" href="https://www.mediawiki.org/w/load.php?modules=mediawiki.legacy.shared|mediawiki.diff.styles&only=styles">
                        <style>
                            body { background: ${dark ? '#0f172a' : '#ffffff'}; color: ${dark ? '#e2e8f0' : '#0f172a'}; padding: 10px; font-size: 0.9rem; }
                            table.diff { width: 100%; }
                        </style>
                    </head>
                `;
                doc.open();
                doc.write(head + '<body>' + html + '</body>');
                doc.close();
                applyDiffFrameTheme();
            })
            .catch(err => {
                doc.body.innerHTML = '<div style="padding:20px; color:red;">Error loading diff</div>';
            });
    }

    function showToast(message, type = 'info') {
        const toastContainer = document.querySelector('.toast-container');
        const toastEl = document.createElement('div');
        toastEl.className = `toast align-items-center text-white bg-${type === 'error' ? 'danger' : (type === 'success' ? 'success' : 'primary')} border-0`;
        toastEl.setAttribute('role', 'alert');
        toastEl.setAttribute('aria-live', 'assertive');
        toastEl.setAttribute('aria-atomic', 'true');

        toastEl.innerHTML = `
            <div class="d-flex">
                <div class="toast-body">
                    ${escapeHtml(message)}
                </div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
            </div>
        `;

        toastContainer.appendChild(toastEl);
        const toast = new bootstrap.Toast(toastEl);
        toast.show();

        toastEl.addEventListener('hidden.bs.toast', () => {
            toastEl.remove();
        });
    }

    // --- Undo Modal Logic ---
    const undoModalEl = document.getElementById('undoModal');
    const undoModal = new bootstrap.Modal(undoModalEl);
    const undoPageTitle = document.getElementById('undoPageTitle');
    const undoSummaryInput = document.getElementById('undoSummary');
    const confirmUndoBtn = document.getElementById('confirmUndoBtn');
    let currentUndoEvent = null;

    undoModalEl.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter') return;
        if (!undoModalEl.classList.contains('show')) return;
        if (e.shiftKey || e.ctrlKey || e.metaKey || e.altKey) return;
        if (e.target && e.target.closest('[data-bs-dismiss="modal"]')) return;
        e.preventDefault();
        if (!confirmUndoBtn.disabled) {
            confirmUndoBtn.click();
        }
    });

    function performUndo(event) {
        currentUndoEvent = event;
        undoPageTitle.textContent = event.title;
        undoSummaryInput.value = ''; // Reset summary
        undoModal.show();
    }

    confirmUndoBtn.addEventListener('click', () => {
        if (!currentUndoEvent) return;

        const summary = undoSummaryInput.value.trim();
        const event = currentUndoEvent;

        const params = new URLSearchParams({
            serverName: event.server_name,
            title: event.title,
            revision: event.revision.new
        });

        if (summary) {
            params.append('summary', summary);
        }

        undoModal.hide();

        undoModal.hide();

        const csrfToken = getCookie('XSRF-TOKEN');
        const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
        if (csrfToken) {
            headers['X-XSRF-TOKEN'] = csrfToken;
        }

        fetch('/api/action/undo', { method: 'POST', body: params, headers: headers })
            .then(res => {
                if (res.ok) return res.json();
                return res.json().then(json => { throw new Error(json.error || 'Unknown error'); });
            })
            .then(data => {
                if (data.error) {
                    throw new Error(data.error.info || data.error.code || 'API Error');
                }
                if (data.edit && data.edit.result && data.edit.result !== 'Success') {
                    throw new Error('Undo failed: ' + (data.edit.info || data.edit.result));
                }
                showToast('Undo successful', 'success');
            })
            .catch(err => showToast('Error: ' + err.message, 'error'));
    });

    function performRollback(event) {
        if (!confirm(`Rollback edits by ${event.user} on "${event.title}"?`)) return;

        const params = new URLSearchParams({
            serverName: event.server_name,
            title: event.title,
            user: event.user
        });

        const csrfToken = getCookie('XSRF-TOKEN');
        const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
        if (csrfToken) {
            headers['X-XSRF-TOKEN'] = csrfToken;
        }

        fetch('/api/action/rollback', { method: 'POST', body: params, headers: headers })
            .then(res => {
                if (res.ok) return res.json();
                return res.json().then(json => { throw new Error(json.error || 'Unknown error'); });
            })
            .then(data => {
                if (data.error) {
                    throw new Error(data.error.info || data.error.code || 'API Error');
                }
                // Rollback response usually doesn't have a 'result' field like edit, it just returns the rollback info or error
                showToast('Rollback successful', 'success');
            })
            .catch(err => showToast('Error: ' + err.message, 'error'));
    }

    document.addEventListener('keydown', (e) => {
        if (typeof isLoggedIn === 'undefined' || !isLoggedIn) return;
        if (!selectedEvent) return;
        if (e.ctrlKey || e.metaKey || e.altKey) return;
        if (isTypingTarget(e.target)) return;
        if (document.querySelector('.modal.show')) return;

        const key = e.key.toLowerCase();
        if (key === 'u') {
            e.preventDefault();
            performUndo(selectedEvent);
        } else if (key === 'r') {
            e.preventDefault();
            performRollback(selectedEvent);
        }
    });

    // --- Navigation (Mobile) ---
    const mobileMenuBtn = document.getElementById('mobileMenuBtn');
    const sidebar = document.querySelector('.event-queue');

    // Create Backdrop
    const backdrop = document.createElement('div');
    backdrop.className = 'menu-backdrop';
    document.body.appendChild(backdrop);

    function toggleMenu(show) {
        if (show) {
            sidebar.classList.add('show-menu');
            backdrop.classList.add('show');
        } else {
            sidebar.classList.remove('show-menu');
            backdrop.classList.remove('show');
        }
    }

    if (mobileMenuBtn) {
        mobileMenuBtn.addEventListener('click', () => toggleMenu(true));
    }

    backdrop.addEventListener('click', () => toggleMenu(false));

    // Update loadDiff to close menu on mobile
    // Helper wrapper so we don't redefine the main function logic repeatedly or confusingly
    const originalLoadDiff = loadDiff;

    // We can't easily redefine loadDiff here because it's defined in the scope above.
    // Instead, let's just create a new function and assign it if we could, but 'function loadDiff' is hoisted or const.
    // It's defined as 'function loadDiff' so it is hoisted.

    // Actually, simpler approach: modify the click handler in addEventToQueue to call toggleMenu(false)
    // AND modify loadDiff to NOT deal with 'showing-diff' class anymore.

    // Let's rewrite the click handler in addEventToQueue by redefining the addEventToQueue function? 
    // No, that's too big.

    // Let's just override the mobileBackBtn listener (which we are replacing)
    // AND we need to handle the fact that 'loadDiff' adds 'showing-diff'.
    // We should remove the 'showing-diff' logic from loadDiff or nullify its effect in CSS (which we did).

    // We need to ensure that when a card is clicked, the menu closes.
    // Since card click calls loadDiff, let's monkey-patch loadDiff? 
    // No, cleaner to just change the logic in a separate replacement if needed.

    // Wait, the previous plan said "Remove .show-menu on item click".
    // I can stick a global listener on eventList?
    eventList.addEventListener('click', (e) => {
        if (e.target.closest('.event-card')) {
            toggleMenu(false);
        }
    });


    // --- Controls ---
    pauseBtn.addEventListener('click', () => {
        const csrfToken = getCookie('XSRF-TOKEN');
        fetch('/api/pause', {
            method: 'POST',
            headers: csrfToken ? { 'X-XSRF-TOKEN': csrfToken } : {}
        });
        pauseBtn.style.display = 'none';
        resumeBtn.style.display = 'inline-block';
    });

    resumeBtn.addEventListener('click', () => {
        const csrfToken = getCookie('XSRF-TOKEN');
        fetch('/api/resume', {
            method: 'POST',
            headers: csrfToken ? { 'X-XSRF-TOKEN': csrfToken } : {}
        });
        resumeBtn.style.display = 'none';
        pauseBtn.style.display = 'inline-block';
    });

    // --- CodeMirror Setup ---
    const knownProperties = [
        'id', 'type', 'namespace', 'title', 'pageId', 'titleUrl', 'user', 'comment', 'parsedcomment',
        'timestamp', 'wiki', 'bot', 'minor', 'patrolled', 'notifyUrl', 'serverUrl', 'serverName',
        'serverScriptPath', 'server_name', 'server_url', 'server_script_path', 'title_url',
        'notify_url', 'added_lines', 'removed_lines', 'user_name', 'page_namespace', 'old_size',
        'user_rights', 'user_groups'
    ];

    const knownFunctions = [
        'contains', 'containsIgnoreCase', 'startsWith', 'endsWith', 'equals', 'equalsIgnoreCase',
        'length', 'isEmpty', 'isBlank', 'matches', 'regexCount', 'count', 'lower', 'upper', 'trim',
        'removeWhitespace', 'normalizeArabic', 'in', 'anyContains', 'allContains', 'hour',
        'dayOfWeek', 'isNightTime', 'test'
    ];

    const keywords = ['true', 'false', 'null', 'and', 'or', 'not', 'new']; // SpEL keywords

    let cmEditor = null;

    if (document.getElementById('filterCode')) {
        const textArea = document.getElementById('filterCode');
        textArea.style.display = 'none'; // Hide original textarea

        cmEditor = new CM.EditorView({
            doc: textArea.value,
            extensions: [
                CM.basicSetup,
                CM.javascript(),
                CM.linter(validateSpEL),
                CM.autocompletion({ override: [spelHint] }),
                CM.keymap.of([CM.completionKeymap]),
                CM.EditorView.updateListener.of(update => {
                    if (update.docChanged) {
                        textArea.value = update.state.doc.toString();
                    }
                }),
                CM.placeholder("wiki == 'enwiki'\ntype == 'edit'")
            ],
            parent: textArea.parentNode
        });
        cmEditor.dom.addEventListener("touchstart", (e) => {
            cmEditor.contentDOM.focus();
        }); 
    }

    function spelHint(context) {
        let word = context.matchBefore(/[\w_]*$/);
        if (!word) return null;
        if (word.from == word.to && !context.explicit) return null;

        const list = [];
        const match = word.text;

        // Add properties
        knownProperties.forEach(p => {
            list.push({ label: p, type: "variable", boost: p.startsWith(match) ? 1 : 0 });
        });

        // Add functions
        knownFunctions.forEach(f => {
            list.push({ label: f + '()', type: "function", apply: f + '()', boost: f.startsWith(match) ? 1 : 0 });
        });

        // Add keywords
        keywords.forEach(k => {
            list.push({ label: k, type: "keyword", boost: k.startsWith(match) ? 1 : 0 });
        });

        return {
            from: word.from,
            options: list
        };
    }

    function validateSpEL(view) {
        const text = view.state.doc.toString();
        const found = [];
        if (!text) return found;

        const lines = text.split('\n');

        lines.forEach((lineText, i) => {
            // Get line start offset
            const lineStart = view.state.doc.line(i + 1).from;

            let currentLine = lineText;

            // Remove strings (simple replacement to preserve length/positions approximately? 
            // Actually replacing with "" changes length and messes up indices if we don't be careful.
            // But replacing 'foo' with '""' changes length.
            // Better to replace with spaces or same length placeholders?
            // The original code was naive: .replace(/'[^']*'/g, '""').
            // This DID change length, so the original `regex.exec(currentLine)` loop's `match.index` 
            // would be wrong if strings were earlier in the line.
            // The original code had a BUG there if strings were involved.
            // Let's improve it slightly by replacing with spaces for validation purposes.

            // Mask strings with spaces
            currentLine = currentLine.replace(/'([^']*)'/g, (m) => " ".repeat(m.length));
            currentLine = currentLine.replace(/"([^"]*)"/g, (m) => " ".repeat(m.length));

            // Remove comments (to end of line)
            const commentIdx = currentLine.indexOf('#');
            if (commentIdx !== -1) {
                // Replaces comment with spaces
                currentLine = currentLine.substring(0, commentIdx) + " ".repeat(currentLine.length - commentIdx);
            }

            // Find identifiers
            const regex = /\b[a-zA-Z_][a-zA-Z0-9_]*\b/g;
            let match;
            while ((match = regex.exec(currentLine)) !== null) {
                const word = match[0];
                const col = match.index;

                // Check if known
                if (!knownProperties.includes(word) &&
                    !knownFunctions.includes(word) &&
                    !keywords.includes(word)) {

                    found.push({
                        from: lineStart + col,
                        to: lineStart + col + word.length,
                        message: "Unknown variable or function: " + word,
                        severity: "error"
                    });
                }
            }
        });

        return found;
    }

    // --- New Filter Logic ---
    const filterListContainer = document.getElementById('filterListContainer');
    const filterFormContainer = document.getElementById('filterFormContainer');
    const filterList = document.getElementById('filterList');
    const showCreateFilterBtn = document.getElementById('showCreateFilterBtn');
    const cancelFilterFormBtn = document.getElementById('cancelFilterFormBtn');
    const filterNameInput = document.getElementById('filterNameInput');
    const editFilterId = document.getElementById('editFilterId');
    const filterFormTitle = document.getElementById('filterFormTitle');

    function loadFilters() {
        if (!filterList) return;
        filterList.innerHTML = '<div class="text-center text-muted p-4"><div class="spinner-border spinner-border-sm"></div> Loading filters...</div>';
        fetch('/api/filters')
            .then(res => {
                if (!res.ok) throw new Error('Failed to load filters');
                return res.json();
            })
            .then(filters => {
                filterList.innerHTML = '';
                if(filters.length === 0) {
                    filterList.innerHTML = '<div class="text-center text-muted p-4">No filters created yet.</div>';
                    return;
                }
                filters.forEach(filter => {
                    const item = document.createElement('div');
                    item.className = 'list-group-item d-flex justify-content-between align-items-center mb-1 rounded border';
                    item.innerHTML = `
                        <div>
                            <span class="fw-bold fs-6">${escapeHtml(filter.name)}</span>
                            <span class="badge ${filter.active ? 'bg-success' : 'bg-secondary'} ms-2">${filter.active ? 'Active' : 'Inactive'}</span>
                        </div>
                        <div class="d-flex gap-2">
                            <button class="btn btn-sm btn-outline-secondary edit-btn" data-id="${filter.id}" title="Edit filter"><i class="bi bi-pencil"></i></button>
                            <button class="btn btn-sm btn-outline-primary toggle-btn" data-id="${filter.id}">${filter.active ? 'Deactivate' : 'Activate'}</button>
                            <button class="btn btn-sm btn-outline-danger delete-btn" data-id="${filter.id}"><i class="bi bi-trash"></i></button>
                        </div>
                    `;
                    // Add listeners
                    item.querySelector('.edit-btn').addEventListener('click', () => editFilter(filter));
                    item.querySelector('.toggle-btn').addEventListener('click', () => toggleFilter(filter.id, filter.active));
                    item.querySelector('.delete-btn').addEventListener('click', () => deleteFilter(filter.id));
                    filterList.appendChild(item);
                });
            })
            .catch(err => {
                filterList.innerHTML = '<div class="text-center text-danger p-4">Error loading filters.</div>';
                showToast(err.message, 'error');
            });
    }

    function toggleFilter(id, currentState) {
        const csrfToken = getCookie('XSRF-TOKEN');
        const headers = { 'Content-Type': 'application/json' };
        if (csrfToken) headers['X-XSRF-TOKEN'] = csrfToken;

        fetch(`/api/filters/${id}/toggle`, {
            method: 'POST',
            headers: headers,
            body: JSON.stringify({ active: !currentState })
        }).then(res => {
            if(!res.ok) {
                res.json().then(data => showToast(data.message || 'Error toggling filter', 'error'));
            } else {
                loadFilters();
            }
        });
    }

    function deleteFilter(id) {
        if(!confirm('Are you sure you want to delete this filter?')) return;
        const csrfToken = getCookie('XSRF-TOKEN');
        const headers = {};
        if (csrfToken) headers['X-XSRF-TOKEN'] = csrfToken;

        fetch(`/api/filters/${id}/delete`, {
            method: 'POST',
            headers: headers
        }).then(res => {
            if(!res.ok) {
                showToast('Error deleting filter', 'error');
            } else {
                loadFilters();
            }
        });
    }

    if (filterBtn) {
        filterBtn.addEventListener('click', () => {
            filterListContainer.classList.remove('d-none');
            filterFormContainer.classList.add('d-none');
            loadFilters();
            filterModal.show();
        });
    }

    function editFilter(filter) {
        filterListContainer.classList.add('d-none');
        filterFormContainer.classList.remove('d-none');
        filterFormTitle.textContent = 'Edit Filter';
        editFilterId.value = filter.id;
        filterNameInput.value = filter.name || '';
        const code = filter.filterCode || '';
        if (cmEditor) {
            cmEditor.dispatch({ changes: { from: 0, to: cmEditor.state.doc.length, insert: code } });
        } else {
            document.getElementById('filterCode').value = code;
        }
    }

    if (showCreateFilterBtn) {
        showCreateFilterBtn.addEventListener('click', () => {
            filterListContainer.classList.add('d-none');
            filterFormContainer.classList.remove('d-none');
            filterFormTitle.textContent = 'Create New Filter';
            editFilterId.value = '';
            filterNameInput.value = '';
            if (cmEditor) {
                cmEditor.dispatch({ changes: { from: 0, to: cmEditor.state.doc.length, insert: '' } });
            } else {
                document.getElementById('filterCode').value = '';
            }
        });
    }

    if (cancelFilterFormBtn) {
        cancelFilterFormBtn.addEventListener('click', () => {
            filterListContainer.classList.remove('d-none');
            filterFormContainer.classList.add('d-none');
            loadFilters();
        });
    }

    if (saveFilterBtn) {
        saveFilterBtn.addEventListener('click', () => {
            const code = cmEditor ? cmEditor.state.doc.toString() : document.getElementById('filterCode').value;
            const name = filterNameInput.value.trim();
            const existingId = editFilterId.value;

            if (!name) {
                showToast('Filter name is required', 'error');
                return;
            }

            const csrfToken = getCookie('XSRF-TOKEN');
            const headers = { 'Content-Type': 'application/json' };
            if (csrfToken) headers['X-XSRF-TOKEN'] = csrfToken;

            saveFilterBtn.disabled = true;
            saveFilterBtn.textContent = 'Saving...';

            const url = existingId
                ? `/api/filters/${existingId}/update`
                : '/api/filters/create';

            fetch(url, {
                method: 'POST',
                headers: headers,
                body: JSON.stringify({ name: name, filterCode: code })
            })
            .then(res => {
                saveFilterBtn.disabled = false;
                if (res.ok) {
                    saveFilterBtn.textContent = 'Saved!';
                    setTimeout(() => {
                        saveFilterBtn.textContent = 'Save Filter';
                        cancelFilterFormBtn.click();
                    }, 800);
                } else {
                    saveFilterBtn.textContent = 'Save Filter';
                    res.json().then(data => showToast(data.message || 'Error saving filter', 'error'));
                }
            })
            .catch(err => {
                saveFilterBtn.disabled = false;
                saveFilterBtn.textContent = 'Save Filter';
                showToast('Network error', 'error');
            });
        });
    }

    // Start
    connect();

});
