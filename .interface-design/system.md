# iOpenPod Interface Design System

Extracted from `GUI/styles.py`, `GUI/widgets/`, and CSS patterns across the codebase.
This project is a **PyQt6 desktop app** — all tokens map to Python constants in `Metrics` and `Colors`.

---

## Spacing

Base unit: **1px** (hairline borders). Practical scale follows 2/3/4/6/8/12/14/16/24.

| Token | Value | Usage |
|-------|-------|-------|
| `xs`  | 2px   | Tight insets, scrollbar thumb |
| `sm`  | 4px   | `padding: 4px 0` (secondary rows), icon nudges |
| `md`  | 6px   | Component gaps, inner padding (`setSpacing(6)`) |
| `base`| 8px   | Standard inter-element gap |
| `lg`  | 12px  | Sidebar padding, combo left-pad |
| `xl`  | 14px  | Grid card spacing (`Metrics.GRID_SPACING`), button H-pad |
| `2xl` | 16px  | Content margins (`setContentsMargins`) |
| `3xl` | 24px  | Section insets, sidebar H-pad |

`setContentsMargins` is 0,0,0,0 in 38/44 uses — layout is margin-less by default; explicit whitespace is added intentionally.

---

## Border Radius

Defined in `Metrics`:

| Token | Value | Usage |
|-------|-------|-------|
| `radius-sm` | 6px | Input fields, small chips |
| `radius`    | 8px | Cards, dialogs (default) |
| `radius-lg` | 10px | Panels, popovers |
| `radius-xl` | 12px | Settings cards, large modals |

---

## Typography

Font families: Segoe UI (Win) / .AppleSystemUIFont (Mac) / Noto Sans (Linux). All sizes in **pt** (scalable via `Metrics.apply_font_scale`).

| Token | pt | Usage |
|-------|----|-------|
| `font-xs`         | 8  | Tech details, section headers, fine print |
| `font-sm`         | 9  | Descriptions, secondary labels, small buttons |
| `font-md`         | 10 | Body text, toolbar buttons, controls |
| `font-lg`         | 11 | Sidebar nav, table headers, setting titles |
| `font-xl`         | 12 | Card titles, title bar text |
| `font-xxl`        | 13 | Device name, stat values |
| `font-title`      | 14 | Dialog titles, page section titles |
| `font-page-title` | 16 | Large page headings (Sync Review, empty states) |
| `font-hero`       | 18 | Settings / backup page title |
| `icon-sm`         | 15 | Small icon labels in cards |
| `icon-md`         | 22 | Badge / backup list icons |
| `icon-lg`         | 40 | Grid item placeholder glyphs |
| `icon-xl`         | 48 | Empty-state decorative glyphs |

---

## Depth Strategy

**Borders-only** — 192 border declarations, 0 `box-shadow` uses.

Depth is communicated through:
1. Surface opacity layering (`SURFACE` → `SURFACE_RAISED` → `SURFACE_HOVER` → `SURFACE_ACTIVE`)
2. `border: 1px solid BORDER` for outlines
3. `border-bottom: 1px solid BORDER_SUBTLE` for dividers/hairlines

Never use drop shadows. Use surface tinting to lift elements.

---

## Color Tokens

Defined in `Colors` (class attributes, replaced in-place by `Colors.apply_theme()`). Always reference by token, never hardcode hex.

| Category | Tokens |
|----------|--------|
| **Background** | `BG_DARK`, `BG_MID` (gradient stops) |
| **Surfaces** | `SURFACE`, `SURFACE_ALT`, `SURFACE_RAISED`, `SURFACE_HOVER`, `SURFACE_ACTIVE` |
| **Accent** | `ACCENT`, `ACCENT_LIGHT`, `ACCENT_DIM`, `ACCENT_HOVER`, `ACCENT_PRESS`, `ACCENT_MUTED`, `ACCENT_SOLID` |
| **Text** | `TEXT_PRIMARY`, `TEXT_SECONDARY`, `TEXT_TERTIARY`, `TEXT_DISABLED` |
| **Borders** | `BORDER`, `BORDER_SUBTLE`, `BORDER_FOCUS` |
| **Semantic** | `SYNC_FREED` (teal storage legend), `MENU_BG` |
| **Playlist** | `PLAYLIST_SMART`, `PLAYLIST_PODCAST`, `PLAYLIST_MASTER`, `PLAYLIST_REGULAR` (RGB tuples) |

Supported themes: `dark`, `light`, `system`, `catppuccin-mocha`, `catppuccin-macchiato`, `catppuccin-frappe`, `catppuccin-latte`.

---

## Component Patterns

### Buttons
```
Height: auto (padding-driven)
Padding: 7px 14px (BTN_PADDING_V / BTN_PADDING_H)
Radius: 6px (BORDER_RADIUS_SM)
Helpers: btn_css(), accent_btn_css(), danger_btn_css()
```

### Grid Cards (`MBGridViewItem`)
```
Size: 172 × 230px (GRID_ITEM_W × GRID_ITEM_H)
Art area: 152px square (GRID_ART_SIZE)
Gap: 14px (GRID_SPACING)
Background tint: dominant artwork color at rgba(r,g,b,30)
```

### Input / Combo
```
Radius: 6px
Padding: 3–6px vertical, 8px horizontal
Helpers: input_css(), combo_css()
```

### Sidebar
```
Width: 220px (SIDEBAR_WIDTH)
Scrollbar: 8px wide (SCROLLBAR_W), 40px min thumb (SCROLLBAR_MIN_H)
Painted via DarkScrollbarStyle (QProxyStyle) — CSS scrollbars unreliable on Windows
```

### Scrollbar
```
Width: 8px
Min thumb: 40px
Painted via DarkScrollbarStyle, not CSS — do not use ::-webkit-scrollbar
```

### Track / Table Rows
```
No fixed row height in CSS; QTableWidget auto-sized
Column widths: context-dependent (track number, title, duration, size)
Selection: SELECTION token background
```

---

## Layout Conventions

- **Main window** uses `QStackedWidget` (4 pages): browse / sync review / settings / backup
- **Zero margins by default** — use explicit spacing tokens when whitespace is needed
- **No blocking UI** — all background work via `ThreadPoolSingleton` + `Worker`/`WorkerSignals`
- `setSpacing(0)` is the default; use `setSpacing(6)` or `setSpacing(8)` for breathing room

---

## CSS Helpers (import from `GUI/styles.py`)

| Helper | Use for |
|--------|---------|
| `btn_css()` | Standard ghost/outline button |
| `accent_btn_css()` | Primary call-to-action |
| `danger_btn_css()` | Destructive actions |
| `input_css()` | Text inputs |
| `combo_css()` | QComboBox dropdowns |
| `link_btn_css()` | Inline text-link buttons |
| `table_css()` | QTableWidget base style |
| `scrollbar_css()` | CSS scrollbar (use sparingly — prefer DarkScrollbarStyle) |
