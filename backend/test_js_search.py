from sources import jamessuckling as js


def test_manual_search_url_is_preserved():
    hint_url = (
        "https://www.jamessuckling.com/search-result?"
        "keyword=La%20Demoiselle%20de%20Sociando-Mallet%20Haut-M%C3%A9doc%202018"
    )
    targets = js._build_search_targets(
        "La Demoiselle de Sociando-Mallet Haut-Medoc",
        2018,
        {"jamessuckling_url": hint_url},
    )
    assert targets
    assert targets[0]["url"] == hint_url
    assert "2018" in targets[0]["query"]


def test_search_query_builder_prioritizes_hint_keyword():
    queries = js._build_search_queries(
        "La Demoiselle de Sociando-Mallet Haut-Medoc",
        2018,
        {
            "jamessuckling_url": (
                "https://www.jamessuckling.com/search-result?"
                "keyword=La%20Demoiselle%20de%20Sociando-Mallet%20Haut-M%C3%A9doc%202018"
            )
        },
    )
    assert queries
    assert queries[0] == "La Demoiselle de Sociando-Mallet Haut-Médoc 2018"


def test_exact_result_ranks_above_other_vintages():
    html = """
    <div>
      <a href="/tasting-notes/146782/la-demoiselle-de-sociando-mallet-haut-medoc-2018">
        <p class="text-lg">La Demoiselle de Sociando-Mallet Haut-Médoc 2018</p>
        <div class="text-gray-400">Saturday, February 6, 2021</div>
      </a>
      <a href="/tasting-notes/329661/la-demoiselle-de-sociando-mallet-haut-medoc-2023">
        <p class="text-lg">La Demoiselle de Sociando-Mallet Haut-Médoc 2023</p>
        <div class="text-gray-400">Thursday, January 8, 2026</div>
      </a>
    </div>
    """
    candidates = js._parse_search_result_candidates(html)
    scored = [
        (
            js._search_candidate_rank(
                "La Demoiselle de Sociando-Mallet Haut-Médoc",
                2018,
                candidate,
                1,
                idx,
            ),
            candidate["title"],
        )
        for idx, candidate in enumerate(candidates, start=1)
    ]
    assert scored[0][0] > scored[1][0]


if __name__ == "__main__":
    test_manual_search_url_is_preserved()
    test_search_query_builder_prioritizes_hint_keyword()
    test_exact_result_ranks_above_other_vintages()
    print("JS search tests passed")
