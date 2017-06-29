##KMB-import
*KMB-import* is a collection of scripts and tools for the batch upload of images
from the National Heritage Board's *Kulturmiljöbild*. A list of image ids to
upload was provided by the organisation and the metadata for these were
pre-fetched using `kmb_massload.py`.

The code is based heavily on pre-existing code in
[lokal-profil/RAA-tools](https://github.com/lokal-profil/RAA-tools). 

### Installation
If `pip -r requirements.txt` does not work correctly you might have to add
the `--process-dependency-links` flag to ensure you get the right version
of [Pywikibot](https://github.com/wikimedia/pywikibot-core/) and
[lokal-profil/BatchUploadTools](https://github.com/lokal-profil/BatchUploadTools).

### Note
This repository was split off from 
[lokal-profil/upload_batches](https://github.com/lokal-profil/upload_batches)
so the history might be a bit mixed up.
