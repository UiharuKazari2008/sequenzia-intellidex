(async () => {
    const md5 = require('md5');
    const cron = require('node-cron');
    const { sqlPromiseSafe, sqlPromiseSimple } = require("./utils/sqlClient");
    const { sendData } = require("./utils/mqAccess");
    const tmdb = require('themoviedb-api-client')('99db16e16ddd3a1995a0069eeca27dc6');
    const tmdbStatic = (await tmdb.configuration()).body;
    let activeMetdataQuery = false;

    // Library Metadata Retrieval
    async function extractShowNames(media_group) {
        const results = (await sqlPromiseSafe(`SELECT kanmi_records.content_full, kanmi_records.attachment_name, kanmi_records.real_filename, kanmi_records.eid FROM kanmi_records, kanmi_channels WHERE kanmi_records.attachment_name != 'multi' AND kanmi_channels.channelid = kanmi_records.channel AND kanmi_channels.media_group = ? AND (kanmi_records.real_filename IS NOT NULL OR kanmi_records.attachment_name IS NOT NULL) ORDER BY kanmi_records.real_filename, kanmi_records.attachment_name;`, [media_group])).rows
        if (results.length > 0) {
            // Transform Filenames
            const filename = results.map(e => {
                return {
                    eid: e.eid,
                    name: ((e.real_filename) ? e.real_filename : e.attachment_name).split('_').join(' ').trim(),
                    orignalName: ((e.real_filename) ? e.real_filename : e.attachment_name)
                }
            })
            // Get Series Names
            let showNames = []
            filename.map(e => {
                const name = e.name.toLowerCase().split(' - ')[0]
                if (showNames.indexOf(name) === -1) {
                    showNames.push(name)
                }
                return false
            })
            //console.log(showNames)
            // Map Files to Series
            let episodeMap = {};
            showNames.map(e => {
                const records = filename.filter(f => f.name.toLowerCase().startsWith(e))
                let special = 0;
                episodeMap[e] = records.filter(f => f.name.split(' - ').length > 1).map(f => {
                    let s = null;
                    let ep = null;
                    const textEnd = f.name.split(' - ')[1].split('.')[0].trim().toLowerCase()
                    if (textEnd.includes('x')) {
                        const parts = textEnd.split('x')
                        const seasonNumber = parseInt(parts[0])
                        const episodeNumber = parseFloat(parts[1])
                        if (!isNaN(seasonNumber) && !isNaN(episodeNumber)) {
                            s = seasonNumber;
                            ep = episodeNumber;
                        } else {
                            console.error(`"${e}" - Could not parse the season or episode number from a "##x##" format, got "${parts.join(" - ")}"`)
                        }
                    } else if (textEnd.includes('s') && textEnd.includes('e')) {
                        const parts = textEnd.split('e');
                        const seasonNumber = parseInt(parts[0].split('s').pop())
                        const episodeNumber = parseFloat(parts[1])
                        if (!isNaN(seasonNumber) && !isNaN(episodeNumber)) {
                            s = seasonNumber;
                            ep = episodeNumber;
                        } else {
                            console.error(`"${e}" - Could not parse the season or episode number from a "S##E##" format, got "${parts.join(" - ")}"`)
                        }
                    } else {
                        const episodeNumber = parseFloat(textEnd.trim())
                        if (!isNaN(episodeNumber)) {
                            s = 1;
                            ep = episodeNumber;
                        } else {
                            console.error(`"${e}" - Could not parse the season or episode number from a "##" flat format, got "${textEnd.trim()}"`)
                        }
                    }
                    if (ep !== Math.floor(ep)) {
                        special++;
                        return {
                            season: 0,
                            episode: special,
                            ...f,
                        }
                    } else {
                        return {
                            season: s,
                            episode: ep,
                            ...f,
                        }
                    }
                })
            })
            return episodeMap
        } else {
            console.log('No records found for that channel');
            return {}
        }
    }
    async function extractMovieNames(media_group) {
        const results = (await sqlPromiseSafe(`SELECT kanmi_records.content_full, kanmi_records.attachment_name, kanmi_records.real_filename, kanmi_records.eid FROM kanmi_records, kanmi_channels WHERE kanmi_channels.channelid = kanmi_records.channel AND kanmi_channels.media_group = ? AND (kanmi_records.real_filename IS NOT NULL OR kanmi_records.attachment_name IS NOT NULL) ORDER BY kanmi_records.real_filename, kanmi_records.attachment_name;`, [media_group])).rows
        if (results.length > 0) {
            // Transform Filenames
            const filename = results.map(e => {
                return {
                    eid: e.eid,
                    name: ((e.real_filename) ? e.real_filename : e.attachment_name).split('_').join(' ').trim(),
                    orignalName: ((e.real_filename) ? e.real_filename : e.attachment_name)
                }
            })
            // Get Series Names
            let movieNames = []
            filename.map(e => {
                const name = e.name.toLowerCase().split(' - ')[0].split('.')[0]
                if (movieNames.indexOf(name) === -1) {
                    movieNames.push(name)
                }
                return false
            })
            //console.log(showNames)
            // Map Files to Series
            let movieMap = {};
            movieNames.map(e => {
                const records = filename.filter(f => f.name.toLowerCase().startsWith(e))
                movieMap[e] = records
            })
            return movieMap
        } else {
            console.log('No records found for that channel');
            return {}
        }
    }
    async function updateShowMetadata(showName, seasonEpisode, adult, existingData) {
        if (showName && seasonEpisode && seasonEpisode.length > 0) {
            return await new Promise(async (resolve) => {
                let returnedMeta = {};
                try {
                    if (existingData && existingData.data) {
                        returnedMeta = existingData.data;
                        returnedMeta.seasons = await Promise.all(existingData.data.seasons.map(async s => {
                            return {
                                ...s,
                                episodes: s.episodes.map(e => {
                                    const episodeData = seasonEpisode.filter(g => g.season === s.number && g.episode === e.number)
                                    return {
                                        ...e,
                                        entity: (episodeData.length > 0 && episodeData[0].eid) ? episodeData[0].eid : undefined
                                    }
                                })
                            }
                        }))
                        resolve(returnedMeta);
                    } else {
                        const overides = (await sqlPromiseSafe(`SELECT show_id FROM kongou_shows_maps WHERE search = ?`, showName)).rows
                        let returnedSearch
                        if (overides.length > 0) {
                            returnedSearch = {
                                results: [ { id: overides[0].show_id } ],
                                total_results: 1
                            }
                        } else {
                            returnedSearch = (await tmdb.searchTv({ query: showName, include_adult: (adult) })).body
                        }
                        if (returnedSearch.total_results > 0) {
                            const returnedShow = returnedSearch.results[0];
                            const show = (await tmdb.tvInfo({ id: returnedShow.id })).body
                            console.log(`Matched the show "${show.name}" => "${showName}"`);

                            // Extract and transform metadata
                            returnedMeta.id = returnedShow.id
                            returnedMeta.name = show.name;
                            returnedMeta.originalName = show.original_name;
                            returnedMeta.description = show.overview;
                            returnedMeta.episodes = show.number_of_episodes;
                            returnedMeta.seasons = show.number_of_seasons;
                            returnedMeta.nsfw = (show.adult);
                            returnedMeta.status = show.status;
                            returnedMeta.rateing = show.vote_average;
                            if (show.backdrop_path) {
                                returnedMeta.background = [
                                    tmdbStatic.images.secure_base_url + 'w780' + show.backdrop_path,
                                    tmdbStatic.images.secure_base_url + 'original' + show.backdrop_path
                                ];
                            }
                            if (show.poster_path) {
                                returnedMeta.poster = [
                                    tmdbStatic.images.secure_base_url + 'w500' + show.poster_path,
                                    tmdbStatic.images.secure_base_url + 'original' + show.poster_path
                                ];
                            }
                            returnedMeta.date = show.first_air_date;
                            returnedMeta.url = show.homepage;
                            returnedMeta.genres = show.genres.map(g => g.name).filter(g => !!g)

                            returnedMeta.seasons = await Promise.all(show.seasons.map(async s => {
                                const returnedSeason = (await tmdb.tvSeasonInfo({
                                    id: returnedShow.id,
                                    season_number: s.season_number
                                })).body
                                return {
                                    number: s.season_number,
                                    name: returnedSeason.name,
                                    description: returnedSeason.overview,
                                    date: returnedSeason.air_date,
                                    poster: [
                                        tmdbStatic.images.secure_base_url + 'w500' + returnedSeason.poster_path,
                                        tmdbStatic.images.secure_base_url + 'original' + returnedSeason.poster_path
                                    ],
                                    episodes: returnedSeason.episodes.map(e => {
                                        const episodeData = seasonEpisode.filter(g => g.season === s.season_number && g.episode === e.episode_number)
                                        return {
                                            number: e.episode_number,
                                            date: e.air_date,
                                            name: e.name,
                                            description: e.overview,
                                            thumbnail: tmdbStatic.images.secure_base_url + 'original' + e.still_path,
                                            durationMinutes: e.runtime,
                                            entity: (episodeData.length > 0 && episodeData[0].eid) ? episodeData[0].eid : undefined
                                        }
                                    })
                                }
                            }))
                            setTimeout(() => { resolve(returnedMeta) }, 1000);

                        } else {
                            console.log('No Results Found for ' + showName)
                            resolve({})
                        }
                    }
                } catch (err) {
                    console.error(`Failed to get metadata for the show ${showName}`)
                    console.error(err);
                    resolve({})
                }
            })
        } else {
            console.error(`Missing required data to get metadata for "${showName}"`);
            return false
        }
    }
    async function updateMovieMetadata(movieName, movieFile, adult) {
        if (movieName && movieFile) {
            return await new Promise(async (resolve) => {
                let returnedMeta = {};
                try {
                    const overides = (await sqlPromiseSafe(`SELECT show_id FROM kongou_shows_maps WHERE search = ?`, movieName)).rows
                    let returnedSearch
                    if (overides.length > 0) {
                        returnedSearch = {
                            results: [ { id: overides[0].show_id } ],
                            total_results: 1
                        }
                    } else {
                        returnedSearch = (await tmdb.searchMovie({ query: movieName, include_adult: (adult) })).body
                    }
                    if (returnedSearch.total_results > 0) {
                        const returnedShow = returnedSearch.results[0];
                        const show = (await tmdb.movieInfo({ id: returnedShow.id })).body
                        console.log(`Matched the movie "${show.title}" => "${movieName}"`)

                        // Extract and transform metadata
                        returnedMeta.id = returnedShow.id
                        returnedMeta.name = show.title;
                        returnedMeta.originalName = show.original_title;
                        returnedMeta.description = show.overview;
                        returnedMeta.nsfw = (show.adult);
                        returnedMeta.rateing = show.vote_average;
                        if (show.backdrop_path) {
                            returnedMeta.background = [
                                tmdbStatic.images.secure_base_url + 'w780' + show.backdrop_path,
                                tmdbStatic.images.secure_base_url + 'original' + show.backdrop_path
                            ];
                        }
                        if (show.poster_path) {
                            returnedMeta.poster = [
                                tmdbStatic.images.secure_base_url + 'w500' + show.poster_path,
                                tmdbStatic.images.secure_base_url + 'original' + show.poster_path
                            ];
                        }
                        returnedMeta.date = show.first_air_date;
                        returnedMeta.url = show.homepage;
                        returnedMeta.genres = show.genres.map(g => g.name).filter(g => !!g)
                        returnedMeta.entity = (movieFile.length > 0 && movieFile[0].eid) ? movieFile.map(e => e.eid) : undefined

                        setTimeout(() => { resolve(returnedMeta) }, 1000);

                    } else {
                        console.log('No Results Found for ' + movieName)
                        resolve({})
                    }
                } catch (err) {
                    console.error(`Failed to get metadata for the movie ${movieName}`)
                    console.error(err);
                    resolve({})
                }
            })
        } else {
            console.error('Missing required data to get metadata');
            return false
        }
    }
    async function updateMetadata(fullUpdate) {
        if (!activeMetdataQuery){
            activeMetdataQuery = true;
            const mediaShows = (await sqlPromiseSafe('SELECT * FROM kongou_shows')).rows
            const mediaGroups = (await sqlPromiseSafe(`SELECT * FROM kongou_media_groups`)).rows;
            let seriesIds = [];
            if (mediaGroups.length > 0) {
                for (let mediaGroup of mediaGroups) {
                    if (mediaGroup.type === 2) {
                        const showList = await extractShowNames(mediaGroup.media_group);
                        for (let k of Object.keys(showList)) {
                            const episodesList = showList[k];
                            const foundShow = mediaShows.filter(e => e.search && e.search === k);
                            const showMeta = await updateShowMetadata(k, episodesList, (mediaGroup.adult === 1), ((foundShow.length > 0 && !fullUpdate) ? foundShow[0] : undefined));
                            if (showMeta && showMeta.id) {
                                if (seriesIds.indexOf(showMeta.id) === -1)
                                    seriesIds.push(showMeta.id);
                                if (foundShow.length === 0 || fullUpdate) {
                                    console.error('New Show ' + showMeta.id + ' added to library');
                                    await sqlPromiseSafe(`INSERT INTO kongou_shows SET ? ON DUPLICATE KEY UPDATE ?`, [
                                        {
                                            show_id: showMeta.id,
                                            media_group: mediaGroup.media_group,
                                            name: showMeta.name,
                                            original_name: showMeta.originalName,
                                            genres: (showMeta.genres && showMeta.genres.length > 0) ? showMeta.genres.join('; ') : null,
                                            data: JSON.stringify(showMeta),
                                            search: k,
                                        },
                                        {
                                            media_group: mediaGroup.media_group,
                                            name: showMeta.name,
                                            original_name: showMeta.originalName,
                                            genres: (showMeta.genres && showMeta.genres.length > 0) ? showMeta.genres.join('; ') : null,
                                            data: JSON.stringify(showMeta),
                                            search: k,
                                        }
                                    ]);
                                }
                                const episodesFound = (await sqlPromiseSafe(`SELECT * FROM kongou_episodes WHERE show_id = ?`, [showMeta.id])).rows;
                                let episodes = []
                                showMeta.seasons.map(s => {
                                    return s.episodes.filter(e => !!e.entity && episodesFound.filter(f => f.eid === e.entity && f.episode_num === e.number && f.season_num === s.number && f.show_id === showMeta.id).length === 0).map(e => {
                                        episodes.push({
                                            season: s.number,
                                            ...e,
                                        })
                                    })
                                })
                                if (episodes.length > 0) {
                                    console.info(showMeta.name + ': found ' + episodes.length + ' new or updated episode(s)');
                                    for (let e of episodes) {
                                        await sqlPromiseSafe(`INSERT INTO kongou_episodes SET ? ON DUPLICATE KEY UPDATE ?`, [
                                            {
                                                eid: e.entity,
                                                show_id: showMeta.id,
                                                episode_num: e.number,
                                                episode_name: e.name,
                                                season_num: e.season,
                                                data: JSON.stringify(e)
                                            },
                                            {
                                                show_id: showMeta.id,
                                                episode_num: e.number,
                                                episode_name: e.name,
                                                season_num: e.season,
                                                data: JSON.stringify(e)
                                            }
                                        ]);
                                    }
                                }
                            }
                        }
                    } else if (mediaGroup.type === 1) {
                        const movieList = await extractMovieNames(mediaGroup.media_group);
                        for (let k of Object.keys(movieList)) {
                            const movieFile = movieList[k];
                            const foundMovie = mediaShows.filter(e => e.search && e.search === k);
                            const movieMeta = (foundMovie.length > 0 && !fullUpdate) ? { ...foundMovie[0].data, entity: (movieFile.length > 0 && movieFile[0].eid) ? movieFile.map(e => e.eid) : undefined } : await updateMovieMetadata(k, movieFile, (mediaGroup.adult === 1));
                            if (movieMeta && movieMeta.id) {
                                if (seriesIds.indexOf(movieMeta.id) === -1)
                                    seriesIds.push(movieMeta.id);
                                if (foundMovie.length === 0 || fullUpdate) {
                                    await sqlPromiseSafe(`INSERT INTO kongou_shows SET ? ON DUPLICATE KEY UPDATE ?`, [
                                        {
                                            show_id: movieMeta.id,
                                            media_group: mediaGroup.media_group,
                                            name: movieMeta.name,
                                            original_name: movieMeta.originalName,
                                            genres: (movieMeta.genres && movieMeta.genres.length > 0) ? movieMeta.genres.join('; ') : null,
                                            data: JSON.stringify(movieMeta),
                                            search: k,
                                        },
                                        {
                                            media_group: mediaGroup.media_group,
                                            name: movieMeta.name,
                                            original_name: movieMeta.originalName,
                                            genres: (movieMeta.genres && movieMeta.genres.length > 0) ? movieMeta.genres.join('; ') : null,
                                            data: JSON.stringify(movieMeta),
                                            search: k,
                                        }
                                    ]);
                                }
                                const moviesFound = (await sqlPromiseSafe(`SELECT * FROM kongou_episodes WHERE show_id = ?`, [movieMeta.id])).rows;
                                const updatedEntitys = movieMeta.entity.filter(e => moviesFound.filter(f => e === f.eid && movieMeta.id === f.show_id).length === 0)
                                if (updatedEntitys.length > 0) {
                                    console.info(movieMeta.name + ': found ' + updatedEntitys.length + ' movie files');
                                    for (let e of updatedEntitys) {
                                        await sqlPromiseSafe(`INSERT INTO kongou_episodes SET ? ON DUPLICATE KEY UPDATE ?`, [
                                            {
                                                eid: e,
                                                show_id: movieMeta.id,
                                                episode_num: null,
                                                episode_name: null,
                                                season_num: movieMeta.entity.indexOf(e),
                                                data: JSON.stringify(e),
                                            },
                                            {
                                                show_id: movieMeta.id,
                                                episode_num: null,
                                                episode_name: null,
                                                season_num: movieMeta.entity.indexOf(e),
                                                data: JSON.stringify(e),
                                            }
                                        ]);
                                    }
                                }
                            } else {
                                console.log(`Failed to match the movie "${movieFile}"`)
                            }
                        }
                    }
                }
                sendData('outbox.discord', {
                    fromClient : `return.IntelliDex`,
                    messageReturn: false,
                    messageChannelID : '0',
                    messageType: 'command',
                    messageAction: 'CacheIDEXMeta',
                }, function (ok) { })
                console.log("Completed Metadata Update!")
            }
            if (seriesIds.length > 0) {
                const cleanUp = await sqlPromiseSimple(`DELETE FROM kongou_shows WHERE (${seriesIds.map(e => "show_id != '" + e + "'" ).join(' AND ')})`)
            }
            await sqlPromiseSimple(`DELETE FROM kongou_episodes WHERE eid IN (SELECT episodes.eid FROM (SELECT e.eid, e.channel, e.show_id, s.media_group FROM (SELECT ep.show_id, rec.* FROM (SELECT eid, show_id FROM kongou_episodes) ep LEFT JOIN (SELECT * FROM kanmi_records) rec ON (ep.eid = rec.eid)) e LEFT JOIN (SELECT * FROM kongou_shows) s ON (e.show_id = s.show_id)) episodes LEFT JOIN (SELECT * FROM kanmi_channels) channels ON (channels.channelid = episodes.channel) WHERE channels.media_group IS NULL)`)
            console.log("Completed Cleanup!")
            activeMetdataQuery = false;
        }
    }

    // Artist Indexer
    async function generateArtistIndex () {
        let foundArtists = [];
        const channels = await sqlPromiseSimple(`SELECT *
                                             FROM kanmi_channels
                                             WHERE classification NOT LIKE '%system%'
                                               AND classification NOT LIKE '%timeline%'
                                               AND parent != 'isparent'`, true);
        const customArtists = await sqlPromiseSimple(`SELECT *
                                                  FROM sequenzia_index_custom`, true);
        if (channels && channels.rows.length > 0) {
            let requests = channels.rows.reduce((promiseChain, ch) => {
                return promiseChain.then(() => new Promise(async (resolve) => {
                    let artistsNames = [];
                    let artists = [];
                    let proccssedEids = [];
                    let cms_decisions = await sqlPromiseSimple(`SELECT userid, SUM(decision)/COUNT(userid) AS percentage FROM twitter_tweets WHERE decision IS NOT NULL GROUP BY userid ORDER BY userid`, true)
                    let messages = await sqlPromiseSafe(`SELECT content_full, attachment_name, real_filename, eid
                                                     FROM kanmi_records
                                                     WHERE channel = ?
                                                     ORDER BY DATE DESC`, [ch.channelid], true)
                    if (messages && messages.rows.length > 0) {
                        const unique = (value, index, self) => {
                            return self.indexOf(value) === index
                        }

                        // Twitter Author Search
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('Twitter Image** - ***') && e.content_full.includes(' (@')).forEach(m => {
                            const a = m.content_full.split(' (@')[1].split(')')[0].toLowerCase().trim()
                            const af = m.content_full.split(' (@')[0].split('***')[1].trim()

                            if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                artists.push({artist: a, name: af, type: 1, source: 1})
                                artistsNames.push(a.toLowerCase());
                                if (af) {
                                    artistsNames.push(af.toLowerCase());
                                }
                                proccssedEids.push(m.eid);
                            }
                        })
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('Twitter Image** - ***') && e.content_full.includes(' (') && !e.content_full.includes(' (@')).forEach(m => {
                            const a = m.content_full.split(' (')[1].split(')')[0].toLowerCase().trim()
                            const af = m.content_full.split(' (')[0].split('***')[1]

                            if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                artists.push({artist: a, name: af, type: 1, source: 1})
                                artistsNames.push(a.toLowerCase());
                                if (af) {
                                    artistsNames.push(af.toLowerCase());
                                }
                                proccssedEids.push(m.eid);
                            }
                        })
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('Twitter Image** - ***') && e.content_full.includes('***') && !e.content_full.includes(' (') && !e.content_full.includes(' (@')).forEach(m => {
                            const a = m.content_full.split('***')[1].toLowerCase().trim()
                            if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                artists.push({artist: a, type: 3, source: 1})
                                artistsNames.push(a.toLowerCase());
                                proccssedEids.push(m.eid);
                            }
                        })
                        // Pixiv User Search
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('**🎆 ') && e.content_full.includes(') - ') && !e.content_full.includes('Twitter Image**')).forEach(m => {
                            try {
                                let content = m.content_full
                                if (m.content_full.includes('🧩 File : ')) {
                                    content = m.content_full.split("\n").filter((e, i) => {
                                        if (i > 1) {
                                            return e
                                        }
                                    }).join("\n")
                                }
                                if (content.includes('**✳️ Related to post')) {
                                    const a = content.split('**🎆')[1].split(') - ')[0].split(' (')[1].toLowerCase().trim()
                                    const ai = content.split('**🎆')[1].split(') - ')[1].split('**')[0].toLowerCase().trim()
                                    const af = content.split('**🎆')[1].split(' (')[0].trim()

                                    if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                        artists.push({artist: a, name: af, id: ai, type: 1, source: 2})
                                        artistsNames.push(a.toLowerCase());
                                        if (af) {
                                            artistsNames.push(af.toLowerCase());
                                        }
                                        proccssedEids.push(m.eid);
                                    }
                                } else {
                                    const a = content.split(') - ')[0].split(' (')[1].toLowerCase().trim()
                                    const ai = content.split(') - ')[1].split('**')[0].toLowerCase().trim()
                                    const af = content.split('**🎆 ')[1].split(' (')[0].trim()

                                    if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                        artists.push({artist: a, name: af, id: ai, type: 1, source: 2})
                                        artistsNames.push(a.toLowerCase());
                                        if (af) {
                                            artistsNames.push(af.toLowerCase());
                                        }
                                        proccssedEids.push(m.eid);
                                    }
                                }
                            } catch (e) {
                                console.error(e)
                                console.log(m.content_full)
                            }
                        })
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('**🎆 ') && e.content_full.includes(')** :') && !e.content_full.includes('Twitter Image**')).forEach(m => {
                            try {
                                let content = m.content_full
                                if (m.content_full.includes('🧩 File : ')) {
                                    content = m.content_full.split("\n").filter((e, i) => {
                                        if (i > 1) {
                                            return e
                                        }
                                    }).join("\n")
                                }
                                const ai = content.split(' (')[1].split(')** ')[0].toLowerCase().trim()
                                const af = content.split('**🎆 ')[1].split(' (')[0].trim()

                                if (artistsNames.indexOf(ai.toLowerCase()) === -1) {
                                    artists.push({artist: ai, name: af, id: ai, type: 2, source: 2})
                                    artistsNames.push(ai.toLowerCase());
                                    if (af) {
                                        artistsNames.push(af.toLowerCase());
                                    }
                                    proccssedEids.push(m.eid);
                                }
                            } catch (e) {
                                console.error(e)
                                console.log(m.content_full)
                            }
                        })
                        // E-Hentai User Search
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('**🐼 ') && e.content_full.includes('** : ***') && !e.content_full.includes('Twitter Image**')).forEach(m => {
                            try {
                                let content = m.content_full
                                if (m.content_full.includes('🧩 File : ')) {
                                    content = m.content_full.split("\n").filter((e, i) => {
                                        if (i > 1) {
                                            return e
                                        }
                                    }).join("\n")
                                }
                                content = content.split("\n")[0];
                                const a = content.split('** : ***')[1].split(' (')[0].split('***')[0].toLowerCase().trim()

                                if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                    artists.push({artist: a, type: 3, source: 5})
                                    artistsNames.push(a.toLowerCase());
                                    proccssedEids.push(m.eid);
                                }
                            } catch (e) {
                                console.error(e)
                                console.log(m.content_full)
                            }
                        })
                        // Kemono/Coomer User Search (NEXT SOURCE WILL BE 25)
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('**🎏 ') && e.content_full.includes('** : ***') && !e.content_full.includes('Twitter Image**')).forEach(m => {
                            try {
                                let content = m.content_full
                                if (m.content_full.includes('🧩 File : ')) {
                                    content = m.content_full.split("\n").filter((e, i) => {
                                        if (i > 1) {
                                            return e
                                        }
                                    }).join("\n")
                                }
                                content = content.split("\n")[0].split("** : ***")[0];
                                const as = content.split(' (');
                                const a = as[0].split('**🎏')[1].toLowerCase().trim();
                                const s = as[1].split(')')[0].toLowerCase().trim();

                                const staticSources = ['fanbox', 'patreon', 'fantia', 'fansly', 'discord', 'afdian', 'boosty', 'gumroad', 'subscribestar', 'onlyfans', 'candfans'];
                                const source = (staticSources.indexOf(s) !== -1) ? (6 + (staticSources.indexOf(s) + 1)) : 6;

                                if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                    artists.push({artist: a, type: ((source === 6) ? 3 : 1), source})
                                    artistsNames.push(a.toLowerCase());
                                    proccssedEids.push(m.eid);
                                }
                            } catch (e) {
                                console.error(e)
                                console.log(m.content_full)
                            }
                        })
                        // Flickr Search
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('https://www.flickr.com') && !e.content_full.includes('Twitter Image')).forEach(m => {
                            try {
                                let content = m.content_full
                                if (m.content_full.includes('🧩 File : ')) {
                                    content = m.content_full.split("\n").filter((e, i) => {
                                        if (i > 1) {
                                            return e
                                        }
                                    }).join("\n")
                                }
                                if (m.content_full.includes('(')) {
                                    const a = content.split(')\n`')[0].split('(').pop().toLowerCase().trim()

                                    if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                        artists.push({artist: a, type: 1, source: 3})
                                        artistsNames.push(a.toLowerCase());
                                        proccssedEids.push(m.eid);
                                    }
                                }
                            } catch (e) {
                                console.error(e)
                                console.log(m.content_full)
                                console.log(m.content_full.split('('))
                            }
                        })
                        // Generic Downloads Search
                        await messages.rows.filter(e => proccssedEids.indexOf(e.eid) === -1 && e.content_full.includes('**🖼 Image** - ***') && e.content_full.includes("' by ") && !e.content_full.includes('Twitter Image**')).forEach(m => {
                            try {
                                let content = m.content_full
                                if (m.content_full.includes('🧩 File : ')) {
                                    content = m.content_full.split("\n").filter((e, i) => {
                                        if (i > 1) {
                                            return e
                                        }
                                    }).join("\n")
                                }
                                const a = content.split(' by ')[1].split('***')[0].toLowerCase().trim()

                                if (artistsNames.indexOf(a.toLowerCase()) === -1) {
                                    artists.push({artist: a, type: 3, source: 4})
                                    artistsNames.push(a.toLowerCase());
                                    proccssedEids.push(m.eid);
                                }
                            } catch (e) {
                                console.error(e)
                                console.log(m.content_full)
                            }
                        })

                        const at1 = artists.filter(a => a.type === 1)
                        const at2 = artists.filter(a => a.type === 2 && at1.filter(b => b.name === a.artist).length === 0 && at1.filter(b => b.artist === a.artist).length === 0)
                        const at3 = artists.filter(a => a.type === 3 && a.artist.length > 3 && at1.filter(b => b.name === a.artist).length === 0 && at1.filter(b => b.artist === a.artist).length === 0)
                        artists = [...at1, ...at2, ...at3]

                        console.log(`Total Artists Found in ${ch.name}: ${artists.length}`)

                        let requests = artists.filter(unique).reduce((promiseChain, at) => {
                            return promiseChain.then(() => new Promise(async (resolveArtist) => {
                                const _cat = customArtists.rows.filter(a => a.artist === at.artist)
                                const _atl = messages.rows.filter(e => (e.content_full.toLowerCase().includes(at.artist.toLowerCase()) || (e.attachment_name && (e.attachment_name.toLowerCase().includes(`${at.artist.toLowerCase()}-`) || e.attachment_name.toLowerCase().includes(`${at.artist.toLowerCase()}_`))) || (e.real_filename && (e.real_filename.toLowerCase().includes(`${at.artist.toLowerCase()}-`) || e.real_filename.toLowerCase().includes(`${at.artist.toLowerCase()}_`)))) || (_cat.length > 0 && (e.content_full.toLowerCase().includes(_cat[0].search.toLowerCase()) || (e.attachment_name && (e.attachment_name.toLowerCase().includes(`${_cat[0].search.toLowerCase()}-`) || e.attachment_name.toLowerCase().includes(`${_cat[0].search.toLowerCase()}_`))) || (e.real_filename && (e.real_filename.toLowerCase().includes(`${_cat[0].search.toLowerCase()}-`) || e.real_filename.toLowerCase().includes(`${_cat[0].search.toLowerCase()}_`))))));
                                const _atc = _atl.length;
                                const _ati = _atl[0].eid;
                                const _ats = at.source;
                                const _atcn = at.type;
                                const _key = `${ch.channelid}-${md5(at.artist)}`;
                                let _search = `artist:${at.artist}`
                                let _url = null;
                                let _name = null;
                                let _artist = null;
                                if (at.source === 1) {
                                    if (at.type === 1) {
                                        _artist = at.artist;
                                        _name = at.name;
                                        _url = `https://twitter.com/${at.artist}/media`;
                                    } else if (at.type === 2) {
                                        _url = `https://twitter.com/${at.artist}/media`;
                                        _artist = at.artist;
                                    } else {
                                        _name = at.artist;
                                    }
                                } else if (at.source === 2) {
                                    if (at.type === 1) {
                                        _artist = at.artist;
                                        _name = at.name;
                                        _url = `https://www.pixiv.net/en/users/${at.id}`;
                                        _search = `artist:${at.id}`;
                                    } else if (at.type === 2) {
                                        _artist = at.id;
                                        _name = at.name;
                                        _url = `https://www.pixiv.net/en/users/${at.id}`;
                                        _search = `artist:${at.id}`;
                                    } else {
                                        _artist = at.artist;
                                        _name = at.name;
                                        _search = `artist:${at.id}`;
                                    }
                                } else if (at.source === 3) {
                                    _name = at.artist;
                                    _url = `https://www.flickr.com/photos/${at.artist}`;
                                } else if (at.source === 4) {
                                    _name = at.artist;
                                } else if (at.source === 5) {
                                    _name = at.artist;
                                    _url = `https://e-hentai.org/?f_search=${_name}`;
                                } else if (at.source >= 6 && at.source <= 24) {
                                    _name = at.artist;
                                    _url = `https://e-hentai.org/?f_search=${_name}`;
                                }
                                if (_cat.length > 0) {
                                    _search += ` OR artist:${_cat[0].search}`
                                }
                                const _atr = cms_decisions.rows.filter(e => _artist && e.userid.toLowerCase() === _artist.toLowerCase()).map(e => e.percentage).pop() || null;

                                const addedArtists = await sqlPromiseSafe(`INSERT INTO sequenzia_index_artists SET ? ON DUPLICATE KEY UPDATE ?`, [{
                                    id: _key,
                                    channelid: ch.channelid,
                                    artist: _artist,
                                    name: _name,
                                    count: _atc,
                                    search: _search,
                                    url: _url,
                                    last: _ati,
                                    source: _ats,
                                    confidence: _atcn,
                                    rating: _atr
                                }, {
                                    count: _atc,
                                    artist: _artist,
                                    name: _name,
                                    last: _ati,
                                    search: _search,
                                    source: _ats,
                                    confidence: _atcn,
                                    rating: _atr
                                }], true);
                                if (!addedArtists) {
                                    console.error(`Failed to write artist data for ${_artist} // ${_name}!`);
                                } else {
                                    foundArtists.push(_key)
                                }
                                resolveArtist();
                            }))
                        }, Promise.resolve());
                        requests.then(() => {
                            console.log(`Pared all artists for ${ch.name}!`);
                            resolve();
                        })
                    } else {
                        console.log(`No Messages Found for ${ch.name}`);
                        resolve();
                    }
                }))
            }, Promise.resolve());
            requests.then(async () => {
                console.log('Index Generated!');
                const artistsToRemove = (await sqlPromiseSimple(`SELECT id FROM sequenzia_index_artists`)).rows.filter(id => foundArtists.indexOf(id.id) !== -1).map(id => `id = '${id}'`).join(' OR ');
                if (artistsToRemove.length > 0) {
                    await sqlPromiseSimple(`DELETE FROM sequenzia_index_artists WHERE (${artistsToRemove})`)
                }
            })
        } else {
            console.log('Failed to get any photo channels')
        }
    }

    await updateMetadata();
    await generateArtistIndex();
    cron.schedule('45 * * * *', async () => { generateArtistIndex(); });
    cron.schedule('*/5 * * * *', async () => { updateMetadata(); });
})()
