"""Localization"""
import iso639


def get_dataset_languages(dataset):
    """Get an ordered list of languages for a dataset in ISO 639-1 format

    :param dataset: Dict containing dataset metadata
    :returns: A list of ISO 639-1 formatted language codes
              eg. ["fi", "en"]
    """
    try:
        languages = dataset["research_dataset"]["language"]
    except KeyError:
        # If 'language' list doesn't exist, use ['en', 'fi'] as a default
        return ["en", "fi"]

    lang_codes = []

    for lang in languages:
        # The value of 'identifier' seems to be
        # 'http://lexvo.org/id/iso639-3/eng'
        lang_id = lang["identifier"].split("/")[-1]
        lang_type = lang["identifier"].split("/")[-2]

        # TODO: Could the 'iso639-2' be also used?
        if lang_type != "iso639-3":
            raise ValueError(
                "Expected iso639-3 language type, got {} instead".format(
                    lang_type
                )
            )

        lang_codes.append(iso639.languages.get(part2t=lang_id).part1)

    return lang_codes


def get_localized_value(dictionary, languages=None):
    """Get localized value from a dict

    :param dictionary: Dict containing a value in multiple languages
    :paramlanguages: A list of ISO 639-1 language codes in order of preference
    :returns: Localized value from dict
    """
    if not languages:
        languages = ["en"]

    # Per MetaX schema, 'und' and 'zxx' are fallbacks for content that
    # can't be localized
    languages += ["und", "zxx"]

    for lang in languages:
        if lang in dictionary.keys():
            return dictionary[lang]

    raise KeyError(f"Localized value not found in {dictionary}")
