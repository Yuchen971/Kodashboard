# KoDashboard

A KOReader plugin that starts a lightweight local web server and exposes a browser dashboard for your reading data.

Open it from any device on the same local network (phone, tablet, laptop) to explore your library, reading progress, statistics, calendar activity, and highlights/notes.

## Screenshots

### Books

<p align="center">
  <img src="./screenshots/Books%20-%20Desktop.jpeg" alt="Books Desktop" width="64%" />
  <img src="./screenshots/Books%20-%20Mobile.png" alt="Books Mobile" width="30%" />
</p>

<p align="center">
  <img src="./screenshots/Books%20Detail%20-%20Desktop.png" alt="Book Detail Desktop" width="82%" />
</p>

### Calendar

<p align="center">
  <img src="./screenshots/Calendar%20-%20Desktop.png" alt="Calendar Desktop" width="64%" />
  <img src="./screenshots/Calendar%20-%20Mobile.png" alt="Calendar Mobile" width="30%" />
</p>

### Stats

<p align="center">
  <img src="./screenshots/Stats%20-%20Desktop.png" alt="Stats Desktop" width="64%" />
  <img src="./screenshots/Stats%20-%20Mobile.png" alt="Stats Mobile" width="30%" />
</p>

### Highlights

<p align="center">
  <img src="./screenshots/Highlights.png" alt="Highlights" width="82%" />
</p>

## Features

- Runs directly inside KOReader
- Local web UI served by the plugin (`web/index.html`, `web/app.js`, `web/style.css`)
- JSON API for books, highlights, stats, overview, and dashboard data
- Bulk cover fetching (`Pull Covers`) with progress, pause/resume, and caching
- Highlights tools: copy single annotation + export all highlights (JSON)
- Configurable port (default `8686`)
- Optional auto-start (for testing purpose)
- Kindle support (opens/closes firewall rules when starting/stopping)

## Installation

### Install from ZIP (recommended)

1. Download the release ZIP.
2. Extract it.
3. Copy the `kodashboard.koplugin` folder into KOReader's `plugins` directory.
4. Restart KOReader.

Expected path:

```text
.../koreader/plugins/kodashboard.koplugin
```

## Usage

1. In KOReader, open the main menu.
2. Open the `KoDashboard` menu.
3. Tap `Start dashboard server`.
4. KOReader will show a local address (for example `http://192.168.1.23:8686`).
5. Open that URL in a browser on another device on the same Wi-Fi/network.

### KOReader plugin menu options

- Start / Stop dashboard server
- Auto start server (test purpose only; not recommended for daily use)
- Port (custom port setting)

## What You Can See

### Books view

- Library in card/table mode
- Title, author, status
- Progress (% + pages read)
- Read time
- Highlight count
- Last opened time
- Search, filter, and sorting

### Book detail view

- Cover and metadata (language, pages, progress)
- Read time / pages read / highlights / notes
- Reading milestones (first open, last open, first annotation)
- Per-book reading heatmap / rhythm
- Full annotation list
- Annotation search + copy

### Calendar view

- Monthly reading heatmap calendar
- Daily reading duration
- Day-level top books
- Monthly summary (read days / total time)
- Streak summary (best/current streak, last read date)

### Stats view

- Reading trend (30/90/180/365-day ranges)
- Best streak / current streak
- Total reading time in selected range
- Books touched in selected range
- Monthly reading time
- Average reading by weekday
- Hourly activity (reading time + sessions)
- Top books by reading time
- Top books by pages read

### Highlights view

- All highlights and notes grouped by book
- Search across title/author/chapter/text/note
- Filter by `All / Highlights / Notes`
- Sort by `Recent / Count / Title`
- Expand/collapse groups
- Copy individual annotation
- Export all highlights as JSON

## Cover Fetching (Pull Covers)

This is one of the core features of KoDashboard.

### How to use it

1. Open the dashboard in your browser.
2. Go to the `Books` tab.
3. Click `Pull Covers`.
4. Let the job run (you can `Pause` / `Resume`).
5. When complete, covers are refreshed in the library UI.

### What the progress panel means

- `saved`: a cover was newly saved (downloaded or extracted)
- `skipped`: a cached cover already exists
- `failed`: cover lookup/download failed
- `Current`: the book currently being processed
- Error list: sample failures (up to a few entries) to help troubleshooting metadata/network issues

### How cover fetching works

- KoDashboard tries local/embedded cover extraction first (when available).
- If no usable local cover is found, it can fall back to Open Library cover lookup/download.
- Downloaded covers are cached, so later runs usually skip existing covers.

### Important notes / caveats

- The KOReader device itself must have network access for online cover fetching.
- Your browser device (phone/laptop) only controls the UI; the fetch runs on the KOReader side.
- Cover matching depends on title/author metadata quality.
- Large libraries can take time depending on library size and network speed.
- Some books may remain without a fetched cover (metadata mismatch, missing result, network failure).

## Highlights: Copy and Download

KoDashboard supports both quick copying and bulk export of annotations.

### Copy a single annotation

From the `Highlights` page or a book detail page:

1. Find an annotation card.
2. Click the copy icon.
3. A formatted text block is copied to your clipboard.

The copied text can include:

- Book title
- Author
- Chapter
- Page
- Date
- Highlight text
- Note text

### Download (export) highlights

From the `Highlights` page:

1. Use search/filter/sort if needed.
2. Click the download icon (Export JSON).
3. KoDashboard downloads a file like:

```text
kodashboard-highlights-YYYY-MM-DD.json
```

The exported JSON includes structured rows such as:

- Book title / author
- Type (`highlight` or `note`)
- Color
- Chapter
- Page
- Datetime
- Highlight text
- Note text

## API Endpoints

All endpoints are `GET` only.

- `/api/books`
- `/api/books/:book_ref`
- `/api/books/:book_ref/annotations`
- `/api/books/:book_ref/timeline`
- `/api/books/:book_ref/cover`
- `/api/books/:book_ref/fetch-cover`
- `/api/highlights`
- `/api/stats`
- `/api/overview`
- `/api/dashboard`

## Notes

- Intended for local network use
- Server binds to all interfaces (`*`) on the configured port
- Static files are served from the plugin `web/` directory
- Only `GET` requests are supported by the built-in server
- `Auto start server` is mainly for testing/debugging and is not recommended for normal use

## Troubleshooting

- If the page does not load, confirm KOReader shows the server as running
- Make sure both devices are on the same network
- Try a different port if `8686` is occupied
- If covers fail to fetch, verify internet access on the KOReader device and check book metadata quality
- On Kindle, restarting the plugin can help if firewall rules were not applied cleanly

## Project Structure

- `main.lua` - plugin entry point and HTTP server
- `api.lua` - API routing and JSON responses
- `dataloader.lua` - data loading/parsing from KOReader data sources
- `web/` - frontend dashboard UI

## Credits

- [Fable](https://fable.co/) (design inspiration for reading/social reading UX ideas)
- [KoInsight by GeorgeSG](https://github.com/GeorgeSG/KoInsight) (inspiration/reference for KOReader reading insights work)
