# luizvbo.github.io

Source for my personal website (https://luizvbo.github.io/), built with
[Zola](https://www.getzola.org/) and the
[tabi](https://github.com/welpo/tabi) theme (vendored in `themes/tabi`).

## Local development

```bash
zola serve          # dev server with live reload
zola build          # build into ./public
```

## Layout

- `content/` — pages and posts; blog posts live in `content/post/<slug>/index.md`
- `content/publication/publications.bib` — BibTeX file rendered by the
  publications page (loaded via `load_data` in `templates/`)
- `sass/custom.scss` — site-specific styles (homepage layout, sticky header)
- `templates/tabi/extend_head.html` — favicon links + analytics snippet
- `zola.toml` — site configuration (menu, skin, taxonomies)

## Deployment

GitHub Actions (`.github/workflows/zola.yml`) builds the site on every push and
deploys to GitHub Pages via `actions/deploy-pages`. Deploys run only from
`master`; other branches and PRs get a build check.
