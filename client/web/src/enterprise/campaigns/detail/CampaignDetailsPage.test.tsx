import React from 'react'
import { CampaignDetailsPage } from './CampaignDetailsPage'
import * as H from 'history'
import { of } from 'rxjs'
import { NOOP_TELEMETRY_SERVICE } from '../../../../../shared/src/telemetry/telemetryService'
import { PageTitle } from '../../../components/PageTitle'
import { registerHighlightContributions } from '../../../../../shared/src/highlight/contributions'
import { mount } from 'enzyme'
import { SupersedingCampaignSpecFields } from '../../../graphql-operations'

// This is idempotent, so calling it in multiple tests is not a problem.
registerHighlightContributions()

const history = H.createMemoryHistory()

describe('CampaignDetailsPage', () => {
    afterEach(() => {
        PageTitle.titleSet = false
    })

    const renderCampaignDetailsPage = ({
        supersedingCampaignSpec,
        viewerCanAdminister,
    }: {
        supersedingCampaignSpec: SupersedingCampaignSpecFields | null
        viewerCanAdminister: boolean
    }) => (
        <CampaignDetailsPage
            namespaceID="namespace123"
            campaignName="c"
            history={history}
            location={history.location}
            isLightTheme={true}
            extensionsController={undefined as any}
            platformContext={undefined as any}
            telemetryService={NOOP_TELEMETRY_SERVICE}
            fetchCampaignByNamespace={() =>
                of({
                    __typename: 'Campaign',
                    id: 'c',
                    url: '/users/alice/campaigns/c',
                    name: 'n',
                    description: 'd',
                    initialApplier: { username: 'alice', url: '/users/alice' },
                    changesetsStats: { total: 10, closed: 0, merged: 0, open: 8, unpublished: 2, deleted: 1, draft: 0 },
                    viewerCanAdminister,
                    branch: 'awesome-branch',
                    createdAt: '2020-01-01',
                    updatedAt: '2020-01-01',
                    closedAt: null,
                    diffStat: {
                        added: 5,
                        changed: 3,
                        deleted: 2,
                    },
                    namespace: {
                        namespaceName: 'alice',
                        url: '/users/alice',
                    },
                    lastAppliedAt: '2020-01-01',
                    lastApplier: {
                        url: '/users/bob',
                        username: 'bob',
                    },
                    currentSpec: {
                        originalInput: 'name: awesome-campaign\ndescription: somestring',
                        supersedingCampaignSpec,
                    },
                })
            }
            queryChangesets={() => of({ totalCount: 0, pageInfo: { endCursor: null, hasNextPage: false }, nodes: [] })}
            deleteCampaign={() => Promise.resolve(undefined)}
            queryChangesetCountsOverTime={() => of([])}
        />
    )

    for (const viewerCanAdminister of [true, false]) {
        for (const supersedingCampaignSpec of [
            null,
            {
                createdAt: '2020-01-01T00:00:00Z',
                applyURL: '/users/foo/campaigns/apply/BAR',
            },
        ]) {
            describe(`supersedingCampaignSpec: ${String(supersedingCampaignSpec)}; viewerCanAdminister: ${String(
                viewerCanAdminister
            )}`, () => {
                test('viewing existing', () => {
                    const rendered = mount(renderCampaignDetailsPage({ supersedingCampaignSpec, viewerCanAdminister }))
                    expect(rendered).toMatchSnapshot()
                })
            })
        }
    }
})
