const {GraphQLNonNull, GraphQLList, getNamedType, GraphQLObjectType, isLeafType} = require('graphql');
const gql = require('graphql-tag');
const {introspectSchema} = require('graphql-tools');
const {ApolloClient} = require('apollo-client');
const {ApolloLink} = require('apollo-link');
const {createHttpLink} = require('apollo-link-http');
const {createPersistedQueryLink} = require('apollo-link-persisted-queries');
const {InMemoryCache} = require('apollo-cache-inmemory');
const fetch = require('node-fetch');
const {isEmpty} = require('lodash');
const {ensureThunkCall} = require('@raychee/utils');


class GraphQLClient {
    constructor(logger, links = [], clientOptions, httpOptions, otherOptions = {resetStoreEvery: 100}) {
        // httpOptions - https://www.apollographql.com/docs/link/links/http/#options
        this.logger = logger;
        this.links = [
            ...links,
            createPersistedQueryLink(),
            createHttpLink({fetch, ...httpOptions}),
        ];
        this.clientOptions = clientOptions;
        this.otherOptions = otherOptions;

        this.apollo = undefined;
        this.counter = Number.MAX_SAFE_INTEGER;
    }

    async _connect() {
        this.schema = await introspectSchema(ApolloLink.from(this.links));
        for (const field of Object.values(this.schema.getQueryType().getFields())) {
            const queryArgDeclare = field.args.map(a => `$${a.name}: ${this._makeFieldTypeExpr(a.type)}`).join(', ');
            const queryArgs = field.args.map(a => `${a.name}: $${a.name}`).join(', ');
            const defaultProjections = this._makeDefaultProjection(getNamedType(field.type));
            this[field.name] = async (logger, variables, projections, options = {}) => {
                logger = logger || this.logger;
                if (!projections || isEmpty(projections)) projections = defaultProjections;
                try {
                    await this._ensureClient();
                    const resp = await this.apollo.query({
                        query: gql`query (${queryArgDeclare}) { ${field.name} (${queryArgs}) ${this._makeReturnExpr(projections)} }`,
                        variables,
                        ...options,
                        context: {logger, ...options.context},
                    });
                    return resp.data[field.name];
                } catch (e) {
                    if (e.networkError && (!e.networkError.statusCode || e.networkError.statusCode >= 500)) {
                        logger.fail('_failed_api_server_error', e);
                    } else {
                        throw e;
                    }
                }
            }
        }
        if (this.schema.getMutationType()) {
            for (const field of Object.values(this.schema.getMutationType().getFields())) {
                const queryArgDeclare = field.args.map(a => `$${a.name}: ${this._makeFieldTypeExpr(a.type)}`).join(', ');
                const queryArgs = field.args.map(a => `${a.name}: $${a.name}`).join(', ');
                const defaultProjections = this._makeDefaultProjection(getNamedType(field.type));
                this[field.name] = async (logger, variables, projections, options = {}) => {
                    logger = logger || this.logger;
                    if (!projections || isEmpty(projections)) projections = defaultProjections;
                    try {
                        await this._ensureClient();
                        const resp = await this.apollo.mutate({
                            mutation: gql`mutation (${queryArgDeclare}) { ${field.name} (${queryArgs}) ${this._makeReturnExpr(projections)} }`,
                            variables,
                            ...options,
                            context: {logger, ...options.context},
                        });
                        return resp.data[field.name];
                    } catch (e) {
                        if (e.networkError && (!e.networkError.statusCode || e.networkError.statusCode >= 500)) {
                            logger.fail('_failed_api_server_error', e);
                        } else {
                            throw e;
                        }
                    }
                }
            }
        }
    }

    async _ensureClient() {
        if (this.counter >= this.otherOptions.resetStoreEvery) {
            this.apollo = new ApolloClient({
                link: ApolloLink.from(this.links),
                cache: new InMemoryCache(),
                ...this.clientOptions,
            });
            this.counter = 0;
        }
        this.counter++;
    }

    _makeFieldTypeExpr(type) {
        if (type instanceof GraphQLNonNull) {
            return `${this._makeFieldTypeExpr(type.ofType)}!`;
        }
        if (type instanceof GraphQLList) {
            return `[${this._makeFieldTypeExpr(type.ofType)}]`;
        }
        return type.name;
    }

    _makeReturnExpr(projections) {
        if (!projections) return '';
        const expr = Object.entries(projections)
            .map(([field, show]) => {
                if (typeof show === 'object') {
                    return `${field} ${this._makeReturnExpr(show)}`;
                }
                if (show) {
                    return field;
                }
            })
            .filter(v => v)
            .join(', ');
        if (expr) {
            return `{${expr}}`;
        } else {
            return '';
        }
    }

    _makeDefaultProjection(returnType) {
        const projection = {};
        if (returnType instanceof GraphQLObjectType) {
            for (const field of Object.values(returnType.getFields())) {
                if (isLeafType(field.type)) {
                    projection[field.name] = true;
                    return projection;
                }
            }
            const [field] = Object.values(returnType.getFields());
            projection[field.name] = this._makeDefaultProjection(getNamedType(field.type));
        }
        return projection;
    }

}


const defaultLogger = {
    info(...args) { console.log(args.join('')); },
    warn(...args) { console.error(args.join('')); },
    fail(code, err, ...args) { 
        if (err instanceof Error && args.length <= 0) throw err; 
        else throw new Error([err, ...args].join('')); 
    },
    crash(code, err, ...args) {
        if (err instanceof Error && args.length <= 0) throw err;
        else throw new Error([err, ...args].join(''));
    },
}


module.exports = {
    type: 'graphql',
    key({links = [], clientOptions, httpOptions, otherOptions = {resetStoreEvery: 100}} = {}) {
        return {links, clientOptions, httpOptions, otherOptions};
    },
    async create({links = [], clientOptions, httpOptions, otherOptions = {resetStoreEvery: 100}} = {}) {
        links = await ensureThunkCall(links, this);
        const client = new GraphQLClient(this, links, clientOptions, httpOptions, otherOptions);
        await client._connect();
        return client;
    },

    async newGraphqlClient({links = [], clientOptions, httpOptions, otherOptions = {resetStoreEvery: 100}, logger = defaultLogger} = {}) {
        links = await ensureThunkCall(links, this);
        const client = new GraphQLClient(logger, links, clientOptions, httpOptions, otherOptions);
        await client._connect();
        return new Proxy(client, {
            get(target, p, receiver) {
                const prop = Reflect.get(target, p, receiver);
                if (typeof prop === 'function' && !p.startsWith('_')) {
                    return prop.bind(target, logger);
                } else {
                    return prop;
                }
            }
        });
        
    },
};
