import App from '@triply/triplydb'
import fs from 'fs'
import path from 'path'

const DATASET = 'imdb'
const GLOB    = /^imdb_shard_.*\.nt\.gz$/ // Upload-Muster

const triply = App.get({ token: process.env.TOKEN }) // API-Token aus Env

async function main() {
    const account = await triply.getAccount()          // eigener Account
    const dataset = await account.getDataset(DATASET)
    //const dataset = await account.addDataset(DATASET)

    // alle passenden Dateien im aktuellen Ordner hochladen
    const cwd = process.cwd()
    const files = fs.readdirSync(process.cwd()).filter(f => GLOB.test(f))
    for (const f of files) {
        const full = path.resolve(cwd, f)
        console.log(`Uploading ${f} ...`)
        await dataset.importFromFiles([full])
        console.log(`Done: ${f}`)
    }
}
main().catch(e => { console.error(e); process.exit(1) })
